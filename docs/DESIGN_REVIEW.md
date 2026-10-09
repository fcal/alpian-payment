# Payment Service — Design Review & Implementation Plan

Review of the initial design brief: inconsistencies flagged, alternatives proposed, gaps
highlighted, followed by a staged implementation plan.

---

## 1. Critical issues

### 1.1 A server-generated UUID does not prevent double spending

The brief says a UUID is generated in the service layer per payment request and used to
"ensure no other transaction is executed while the current one isn't complete".

This does not protect against the most common real double-spend: **a client retry**. If the
client POSTs, the connection times out, and the client POSTs again, the server generates a
*new* UUID and happily executes a *second* payment. The server-generated id is a *correlation*
id, not an idempotency key.

**Fix:** a client-supplied `Idempotency-Key` header, stored with a `UNIQUE` constraint. On a
repeat key, return the original result (200 + stored response) instead of re-executing. This
is the single most important omission in the brief.

Keep the server-side UUID as the `PAYMENT.ID` — that part of the brief is good, it makes the
journal traceable end to end and gives the Kafka events a natural anchor.

### 1.2 Application-level "is there an ongoing transaction?" is a race, not a lock

The brief puts the concurrency guard in the service layer: "validate that there is no ongoing
transaction for the same user already".

Check-then-act in application code is not atomic. Two threads both read "no ongoing
transaction", both pass the balance check, both debit. An in-JVM lock
(`ConcurrentHashMap<accountId, Lock>`) fixes it for one instance and breaks the moment you run
two replicas — and "High Availability" is a stated focus area, which implies more than one.

**Fix — push the guard into the database.** Two defensible options:

| Approach | Mechanism | Trade-off |
|---|---|---|
| **Pessimistic (recommended)** | `SELECT ... FROM account WHERE id = ? FOR UPDATE` at the top of the transaction | Serializes per account, parallel across accounts, correct across replicas, gives the "queue behind the in-flight payment" behaviour the brief describes. Needs a `lock_timeout`. |
| Optimistic | `UPDATE account SET balance = balance - :amt, version = version + 1 WHERE id = :id AND version = :v AND balance >= :amt`, check `rowsAffected`, retry | No lock held; needs retry logic; degrades under contention on a hot account. |

Either way, add `CHECK (balance >= 0)` on `ACCOUNT` so the database is the final authority. No
code path can overdraft, even a buggy one. That constraint is worth more than any amount of
service-layer validation.

### 1.3 "Ongoing transaction" state is vestigial in a synchronous design

This is worth settling before writing code, because two different architectures are mixed in
the brief.

**Design A — synchronous (recommended).** One DB transaction does: lock account → check
balance → debit → insert `PAYMENT` (terminal status) → insert `OUTBOX` row. Commit. The API
returns `201` with the final outcome. Concurrency is handled entirely by the row lock. Because
the transaction is short (single-digit ms) and atomic, **no payment is ever observably
"ongoing"** — there is no state for a concurrent request to find. The `ongoing_transaction`
concept has nothing to do.

**Design B — reservation / two-phase.** `PAYMENT` is inserted `PENDING`, funds are reserved, an
external payment rail is called, and a later confirmation moves it to `COMPLETED`/`FAILED`.
*Here* "block new payments while one is ongoing" earns its keep, because the window is long and
externally bounded.

**Recommendation: Design A.** It satisfies every requirement in the brief (balance check,
concurrency, Postgres journal, Kafka notification, outbox) with far less machinery and far less
to get wrong. Document Design B in the README as the extension path for a real payment rail.
Avoid modelling a `PENDING` status that is never observable — vestigial states confuse readers.

### 1.4 The two-topic join is the outbox pattern reinvented, but worse

The brief proposes: service publishes to `ongoing_transaction`; Postgres CDC publishes to
`completed_transaction`; the notification module joins both and notifies only when a UUID
appears in both.

The join exists only to compensate for a **dual write** — publishing to Kafka outside the DB
transaction, where the event can be sent and the transaction then rolled back. The transactional
outbox eliminates that problem at the source: the event row is inserted *in the same transaction*
as the debit, so **the event already means "the transaction committed"**. There is nothing left
to join against. `ongoing_transaction` carries no information that `payment-events` doesn't.

The brief itself names the outbox pattern as a main part of the task — the join contradicts it.

**Recommendation: one topic, `payment-events`. Drop the join.**

### 1.5 Compacted topics with a unique key compact nothing

The brief proposes key = `userId + transactionId` on compacted topics, "so we'll only have the
latest state of each transaction".

Two problems:

- **Compaction retains the latest value *per key*.** If every key is unique, compaction retains
  *every record, forever*. The topic grows without bound. Compaction is for keyed **state**
  (latest status per account), never for an **event stream**.
- **A transaction-id in the key destroys per-account ordering.** Unique keys hash across all
  partitions, so two payments on the same account land on different partitions and can be
  processed out of order. Any future per-account stateful processing is broken.

**Recommendation:**
- Partition key = `accountId`. Per-account ordering, co-location, and co-partitioning with the
  consumer's dedup state store.
- `paymentId` in the value, and also as a Kafka **header** for cheap dedup and tracing.
- `cleanup.policy=delete`, `retention.ms` ≈ 7 days. Events are facts, not state.
- If a compacted topic is wanted, the legitimate place is a separate `payment-status` topic
  keyed by `paymentId` holding latest status. Optional, not needed for the notification flow.

---

## 2. Data model corrections

The proposed schema is a reasonable start but cannot currently represent a payment.

### Missing from `PAYMENT`
- **Beneficiary / counterparty.** A payment has a sender *and* a receiver. With only
  `ACCOUNT_ID` the row records "money left this account" and nothing about where it went.
- **`STATUS`** (`COMPLETED` / `FAILED`) — needed for the event payload and for a status query.
- **`IDEMPOTENCY_KEY`** with a `UNIQUE` constraint (see 1.1).
- **`FAILURE_REASON`** for the failed case.

### Decide: outbound payment vs internal transfer
These need different schemas:
- **Outbound** (money leaves to an external beneficiary): a single `ACCOUNT_ID` plus
  beneficiary details is *correct*. Simplest, and makes the one-sided row defensible.
- **Internal transfer** (account → account): inherently two-sided. Either two `PAYMENT` rows
  (debit + credit) or a `(debit_account_id, credit_account_id)` pair. And it needs
  **deadlock avoidance** — two concurrent transfers A→B and B→A will deadlock unless both
  transactions lock accounts in a deterministic order (e.g. ascending account id). That is a
  genuinely interesting detail to demonstrate, at the cost of more complexity.

Real payment systems are **double-entry**: every movement is a balanced pair, and
`ACCOUNT.BALANCE` is a materialized cache of the ledger sum. Worth one paragraph in the README
acknowledging this, even if we keep balance-as-column for the assignment.

### Other schema fixes
- **`USER` is a reserved word in PostgreSQL.** Rename to `app_user` (or `users`), or quote it
  everywhere forever.
- **Money type:** `NUMERIC(19,4)` in Postgres, `BigDecimal` in Java. Never `double`/`float`.
  (Alternative: `BIGINT` minor units — avoids rounding questions entirely, slightly less
  readable. `NUMERIC` + `BigDecimal` is the better default here.)
- **`TIMESTAMPTZ`, not `TIMESTAMP`.** Easy to miss, and it matters.
- **Currency rule is unspecified.** `ACCOUNT.CURRENCY` and `PAYMENT.CURRENCY` can currently
  diverge. State the rule explicitly: reject when they differ; no FX in scope.
- **`CHECK (amount > 0)`.** Without it, a negative amount is a free deposit.
- **`CHECK (balance >= 0)`** — the last line of defence (see 1.2).
- Index on `PAYMENT (account_id, created_at DESC)` for history queries.

### Confirmed good choices
- `PAYMENT.ID` = the request UUID.
- Journal semantics: never delete or update a payment row.
- One user → many accounts; one account → one user; one payment → one account.

---

## 3. ORM vs native SQL

| | Pros | Cons |
|---|---|---|
| **Spring Data JPA / Hibernate** | Fast CRUD, entity mapping, `@Version`, `@Lock(PESSIMISTIC_WRITE)` | Hidden SQL, N+1, flush-timing surprises. Critically: **the natural JPA idiom is read-entity → mutate → flush**, which is exactly the read-modify-write race we must avoid. You end up fighting the ORM with `@Query` native SQL anyway. First-level cache can serve stale state. |
| **Spring Data JDBC** | Aggregate mapping, no lazy loading, no dirty checking, SQL stays visible | Less flexible than raw SQL for conditional updates and `SKIP LOCKED` |
| **`JdbcClient` / `JdbcTemplate` + native SQL** | Full control over `FOR UPDATE`, single-statement conditional updates, `SKIP LOCKED`; trivially reviewable; keeps the domain model free of persistence annotations | More mapping boilerplate, no compile-time query safety |

**Recommendation: `JdbcClient`** (Spring Framework 6.1, available in Boot 3.3) with hand-written
SQL.

The reasoning is the point of the assignment: this task *is* about the concurrency primitives.
`SELECT ... FOR UPDATE`, conditional atomic `UPDATE`, and `SKIP LOCKED` read clearly in SQL and
get obscured by an ORM. The schema is four tables with no graph traversal, so an ORM buys very
little. And the repository abstraction (Postgres impl + in-memory impl) stays clean when no JPA
annotations leak into the domain types.

Document this choice and its rejected alternative in the README — the reasoning is part of the
deliverable.

---

## 4. Repository layer — one important correction

The brief wants the repository interface to "abstract all logic and simply define the base
operations", with business logic in the service layer.

**If the interface is CRUD, the race is unavoidable in every implementation.** A
`findAccount()` + `save()` pair forces the service to do read-then-write, and no amount of
service-layer care makes that atomic.

**Design the interface around intent, not CRUD:**

```java
// Atomic: locks the account, verifies funds, debits, and records the payment.
// Returns empty when funds are insufficient — the caller decides what that means.
Optional<DebitResult> debitIfSufficientFunds(AccountId id, Money amount, PaymentId paymentId);
```

**Resolving the layering tension:** *policy* stays in the service (is this payment allowed?
which error code? which message?); the repository exposes one atomic primitive and returns a
result the service interprets. The service still pre-validates for fast, clear errors; the
repository's conditional update is the *authoritative* guard. Both layers keep their jobs.

### The in-memory implementation has a hard limit — be honest about it

An in-memory map **cannot reproduce `SELECT FOR UPDATE` semantics or transaction rollback**. It
will pass tests real Postgres would fail, and vice versa. Scope it honestly:

- **In-memory impl tests:** business rules — insufficient funds, currency mismatch, validation,
  mapping, error selection. No DB needed, fast.
- **Concurrency is testable only against real Postgres** (Testcontainers): N threads hammering
  one account, asserting final balance and payment count. **This test is the centrepiece of the
  submission** — it is the one that proves the headline requirement.

---

## 5. Outbox relay: poller vs Debezium

The brief asks whether out-of-the-box Postgres→Kafka connectors exist. They do:

- **Debezium** — CDC via Postgres logical decoding (the WAL). Emits *row-change* events with
  raw column names: a leaky, schema-coupled contract. Needs Kafka Connect, `wal_level=logical`,
  a publication and replication slot, and converters.
- **Confluent JDBC Source Connector** — polling-based; needs a monotonic column; weaker
  guarantees.

**Debezium + the outbox table together is the canonical production setup**: Debezium watches
*only* the `OUTBOX` table and the `EventRouter` SMT routes rows to topics. Clean contract, no
poller.

**Recommendation for this assignment: outbox + an in-service poller relay.**

- `OUTBOX` row written in the same transaction as the debit (atomic — no dual write).
- A `@Scheduled` relay publishes unpublished rows and marks them published.
- **For HA across replicas:** claim rows with
  `SELECT ... FOR UPDATE SKIP LOCKED LIMIT :n` — each replica grabs a disjoint batch, no
  double-publish, no coordination. One line, idiomatic, and exactly the right tool.
- Publish-then-mark gives **at-least-once**, so the consumer must be idempotent — which is
  precisely the "no double notifications" requirement, handled in §6.
- `attempts` + `next_attempt_at` columns give retry with backoff; after N attempts, park the row
  so one poison event can't block the queue (`SKIP LOCKED` makes this natural).

Rationale: zero extra infrastructure, trivially testable, and it demonstrates the pattern
explicitly rather than delegating it to a connector. Document the Debezium path in the README
as the production alternative, with the mapping spelled out.

---

## 6. Kafka Streams vs plain consumer — and the real justification

The brief leans Kafka Streams. That is the right call, but **for a different reason than the
brief gives**.

With a single topic, a plain `@KafkaListener` plus a dedup check would be simpler — Streams is
only justified by *stateful* operations. The justification here is **deduplication**:

- `KStream.process()` with a `KeyValueStore<PaymentId, Long>` of already-notified payment ids.
- A punctuator purges entries older than the retention window, so the store stays bounded.
- `processing.guarantee=exactly_once_v2` makes the store update and the output record atomic.

That is a genuine, defensible use of Streams, and `TopologyTestDriver` makes it very testable.

**Be precise about the guarantee:** EOS v2 covers Kafka→Kafka *and the state store*. It does not
cover the actual notification side effect (an email, an SMS) — that remains at-least-once in the
real world, with the store making it *effectively* once. Stating this precisely is better than
claiming exactly-once delivery.

**A cleaner framing if the two-topic join is kept anyway:** a single `payment-events` topic
consumed by a Streams app that deduplicates and emits `notification-events`. That still exercises
Streams, a state store, and `TopologyTestDriver` — everything the brief wanted to demonstrate —
without the dual write.

---

## 7. Protobuf events — two traps

Protobuf is a good choice (compact, evolvable, consistent with the reference repo). Watch for:

- **No decimal type in protobuf.** Do not use `double` for money. Use a `Money` message with
  `string units` (decimal string) or `int64 minor_units`, plus an ISO-4217 `currency` string.
- **Timestamps:** `google.protobuf.Timestamp`.
- **No Schema Registry** means hand-rolling a small `ProtobufSerde`. Fine — note Confluent
  Schema Registry as the production answer and that it's skipped to keep the container count
  down.

---

## 8. Error handling & fault tolerance — gaps

"Error Handling" is a stated focus area, but the brief specifies no error contract at all.

- **HTTP contract** via RFC 9457 `ProblemDetail` (built into Spring 6) and a
  `@RestControllerAdvice`:
  | Condition | Status |
  |---|---|
  | Malformed / invalid body, non-positive amount | `400` |
  | Unknown account | `404` |
  | Insufficient funds | `409` |
  | Idempotency key reused with a different payload | `422` (see Stage 3 findings) |
  | Currency mismatch | `422` |
  | Lock timeout / contention | `503` + `Retry-After` |
- **Account ownership check.** `userId` in the path is only meaningful if the service verifies
  the account belongs to that user. Without it the path is a trivial IDOR. This is business
  logic, not auth — it should exist and be tested even with auth out of scope.
- **`lock_timeout` / `statement_timeout`** so a stuck lock doesn't exhaust the connection pool.
  Map the timeout to `503`.
- **Consumer-side DLT:** Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`;
  in Streams, a `DeserializationExceptionHandler` (`LogAndContinue`) and a production exception
  handler. *Revised in Stage 5: `LogAndContinue` drops the record, so decoding moved into the
  processor, which dead-letters instead — see the Stage 5 findings.*
- **Graceful degradation — the headline fault-tolerance test.** If Kafka is down, payments must
  still succeed. That is exactly what the outbox buys, and it is directly demonstrable: stop the
  Kafka container → POST a payment → assert `201` and an unpublished outbox row → restart Kafka →
  assert the event arrives. Very convincing in a review.
- **Actuator** `/actuator/health` with DB and Kafka indicators; distinguish liveness from
  readiness (a payment service should stay *live* but go *not-ready* if Postgres is unreachable).
- **Observability:** payment UUID into the MDC, structured logging, correlation end to end.

---

## 9. API design

```
GET  /api/v1/users/{userId}/accounts/{accountId}/balance
POST /api/v1/users/{userId}/accounts/{accountId}/payments   [Idempotency-Key: <uuid>]
GET  /api/v1/payments/{paymentId}
```

- `userId` in the path with a comment that it must come from a validated JWT subject in
  production — as the brief asks.
- **`GET /payments/{paymentId}` is missing from the brief** and is needed: a client whose request
  times out has no other way to resolve whether its payment happened.
- **Separate DTOs from persistence models**, as the brief says — with explicit mappers. Keep
  mappers hand-written: at this scale they are clearer than MapStruct and have no build-time cost.
- **OpenAPI:** `springdoc-openapi-starter-webmvc-ui` → Swagger UI plus a generated spec. Satisfies
  "Document the REST API". (Spec-first with `openapi-generator`, as in the reference repo, is the
  alternative; generated-from-code is proportionate here.)

---

## 10. Local run & Docker

- **`spring-boot-docker-compose`** (Boot 3.1+): a root `compose.yaml` plus that dependency means
  `./gradlew bootRun` starts Postgres and Kafka and wires the datasource automatically. This is
  literally the "containers spawn when we launch the service" requirement, with near-zero config.
- **Kafka in KRaft mode** (`apache/kafka`) — no Zookeeper, one less container.
- **Topic creation:** an init container running `kafka-topics.sh` from `docker/kafka/create-topics.sh`
  (ops-realistic, as the brief asks). `NewTopic` beans via `KafkaAdmin` are the simpler
  alternative — note it.
- **Flyway** with plain `.sql` files in `db/migration` — matches the "dedicated folder with
  migration scripts" requirement, readable, auto-applied by Boot. (Note in the README that
  production usually decouples migration from application start.)
- **No security, stated explicitly.** PLAINTEXT Kafka, no SASL/TLS, no API authentication — a
  deliberate scope exclusion, called out in the README. Good instinct in the brief; it reads well.

---

## 11. Module structure

```
alpian.payment/
├── proto/                  # protobuf event definitions (shared contract)
├── payment-service/        # API + service + repository + outbox relay
├── notification-service/   # Kafka Streams dedup + notification sink
├── db/migration/           # Flyway SQL
├── docker/                 # compose.yaml, kafka topic scripts
└── docs/                   # design decisions
```

Lean on purpose — a separate `model` or `common` module isn't worth it at this size. Mirror the
reference repo's conventions: Gradle version catalog, spotless + google-java-format, Lombok,
JUnit 5, and a separate `src/component` source set for Testcontainers-based component tests.

---

## 12. Inconsistencies, consolidated

| # | Issue | Severity |
|---|---|---|
| 1 | Server-generated UUID presented as double-spend protection; a client retry defeats it | **High** |
| 2 | Concurrency guard in application code is check-then-act — a race | **High** |
| 3 | Two-topic join is a workaround for a dual write that the outbox eliminates — and the brief names the outbox as a main part | **High** |
| 4 | Compacted topic + unique key ⇒ unbounded retention, compacts nothing | **High** |
| 5 | `userId + transactionId` key destroys per-account ordering | **High** |
| 6 | `PAYMENT` has no beneficiary and no status — cannot represent a payment | **High** |
| 7 | "Ongoing transaction" state is vestigial in a synchronous design; two architectures are mixed | Medium |
| 8 | "Processed in the order received" is not achievable over HTTP — restate as *serialized* | Medium |
| 9 | In-memory repo cannot test concurrency, but is positioned as the unit-test backbone | Medium |
| 10 | `USER` is a PostgreSQL reserved word | Medium |
| 11 | No rule relating `ACCOUNT.CURRENCY` to `PAYMENT.CURRENCY` | Medium |
| 12 | No error contract, though "Error Handling" is a stated focus area | Medium |
| 13 | "High Availability" is a focus area, but the design has single-instance assumptions (in-JVM state, poller double-publish) | Medium |
| 14 | Money type unspecified — `double` anywhere is a defect | Medium |

On #8: HTTP requests arrive concurrently over many connections with no global arrival order, and
Postgres lock wakeups are approximately but not contractually FIFO. The achievable and
sufficient guarantee is **serialization with no lost updates and no overdraft**. True ordering
would require partitioning requests by account onto a single consumer (Kafka keyed by
`accountId`), which forces the API to become asynchronous (`202 Accepted`) — a much larger
change, and overkill here.

---

## 13. Also missing

- **A README documenting design decisions and trade-offs.** For an assignment graded on design,
  this is a primary deliverable, not an afterthought — it is where the locking strategy, the
  ORM choice, outbox-vs-CDC, key design, and the Streams justification get argued.
- Pagination, if a payment-history endpoint is added.
- Optimistic `version` column, if that route is chosen over pessimistic locking.
- HikariCP sizing relative to lock hold time.
- A concurrency/load test as the headline test (see §4).
- **GitHub Actions CI** running build + tests. Cheap, and the repo is public anyway.
- Spotless / google-java-format for consistent style.

---

## 14. Staged implementation plan

Each stage is independently committable, demoable, and green — so the git history itself shows
the progression.

### Stage 0 — Foundations
Multi-module Gradle (version catalog, spotless, jacoco), root `compose.yaml` (Postgres + Kafka
KRaft), Flyway migrations for the corrected schema, README skeleton with the decision log.
**Exit:** `./gradlew build` green; `bootRun` brings up containers; schema applied.

### Stage 1 — Domain, repository contract, in-memory impl
Domain types (`Money`, `Account`, `Payment` as records), intent-based repository interfaces
(§4), in-memory implementations, unit tests for business rules.
**Exit:** service logic fully tested with no database.

### Stage 2 — PostgreSQL implementation
`JdbcClient` implementations: `FOR UPDATE` debit, conditional atomic update, `SKIP LOCKED`
outbox claim. The payment path becomes `@Transactional`, with the lock timeout applied as
`SET LOCAL` inside the transaction (§16.2). Testcontainers integration tests including **the
N-thread single-account concurrency test**.

**Exit:** concurrency test green; balance never negative; exactly one payment per idempotency key.

**Required, and specifically not provable in Stage 1:** with N concurrent requests sharing one
idempotency key, the balance must fall by *exactly one* payment. Stage 1 showed that the service
debits before appending to the journal, so every thread that loses the unique-key race has
already moved money; only a transaction rolls those debits back. The in-memory doubles have no
transaction, so they genuinely leave the account debited once per thread with a single journal
entry — money gone with no record. Stage 1's test therefore asserts only that the key guard
admits one writer, and this invariant is deferred here. It is the single most important
assertion in the suite: it is the one that actually demonstrates that double spending is
prevented.

**Findings during implementation.** Three things surfaced that the plan did not anticipate:

1. **Spring does not translate a PostgreSQL `lock_timeout` into `CannotAcquireLockException`.**
   The driver raises a plain `PSQLException` rather than a JDBC 4 subclass, and the fallback
   translator does not recognise SQLSTATE class `55`, so `55P03` arrived as
   `UncategorizedSQLException`. The service's catch block would never have matched, and lock
   contention would have surfaced as an opaque 500 instead of a retryable rejection. Found by
   asserting the exception type empirically rather than from documentation; fixed with a custom
   translator in `JdbcConfiguration`, which the integration tests share via a static factory so
   they observe exactly the production mapping.
2. **The transaction boundary has to sit below the service, not on it.** A unique-constraint
   violation or lock timeout aborts the PostgreSQL transaction, so neither can be turned into a
   result inside it. `PaymentExecutor` is the transactional core; `PaymentService` wraps it
   non-transactionally and handles those two outcomes after rollback. A separate bean is also
   what avoids Spring's self-invocation trap, which would leave `@Transactional` silently inert —
   and an inert transaction disables the row lock too.
3. **`FOR UPDATE` outside a transaction fails silently.** In autocommit the lock is released the
   moment the select returns, so the code appears to work while providing no protection.
   `PostgresAccountRepository.debit` therefore refuses to run without an active transaction, and
   a context test confirms the container proxies `PaymentExecutor` so that guard never fires in
   practice.

The deferred Stage 1 assertion holds: 32 concurrent requests sharing one idempotency key yield
one completed payment, 31 replays, one journal entry, and a balance reduced by exactly one
payment.

### Stage 3 — REST API
DTOs and mappers, controller, `@RestControllerAdvice` + `ProblemDetail`, idempotency handling,
account-ownership check, springdoc OpenAPI.
**Exit:** documented API, `@WebMvcTest` coverage of every error path, Swagger UI live.

**Findings during implementation.**

1. **A replay could leak another user's payment.** Stages 1 and 2 checked the idempotency key
   *before* verifying account ownership. A user addressing someone else's account with a key that
   account had used received the stored payment — beneficiary, amount, reference — as a "replay".
   Ownership is now established before anything is read on the account's behalf, with regression
   tests at both the service and HTTP layers. Worth recording that the API design surfaced this,
   not the earlier stages' tests: those tested replay and ownership separately, never together.
2. **Key reuse with a different payload was silently replayed.** A client reusing a key for a
   new payment would have been told that payment succeeded when it was never attempted. It is now
   refused with `422` and `code: idempotency_key_reused`. The comparison is on normalised values,
   so `250.0` versus `250.00` or a re-spaced IBAN is still recognised as the same request. `422`
   rather than the `409` in §8, following the IETF `Idempotency-Key` header draft, which reserves
   `409` for a concurrent request still in flight.
3. **The response `code` must not be the raw outcome.** Mapping `PaymentOutcome` straight to the
   problem `code` would have sent `account_not_owned` to the client — undoing the non-disclosure
   the service carefully preserves. Both cases render as `account_not_found`; a test asserts the
   two responses are byte-identical.
4. **Money rendered as a JSON number by default.** `BigDecimal` serialises as a number unless told
   otherwise, which would put amounts straight into the client's floating point. Fixed with
   `@JsonFormat(shape = STRING)`; the test asserts on raw response text, since `jsonPath` coerces
   numbers and strings to the same value and would pass with the defect present.
5. **Framework problems arrived without a `code`.** Spring's base handler builds the
   `ProblemDetail` inside `handleExceptionInternal`, after an override inspecting `body` has
   already run, so enrichment has to happen on the response `super` returns.

**Deviation from §9.** Payment retrieval is
`GET /api/v1/users/{userId}/accounts/{accountId}/payments/{paymentId}` rather than the top-level
`/api/v1/payments/{paymentId}`, so the same ownership check applies. A top-level path would have
needed the check to be reconstructed from the payment alone, and the account match matters too:
without it, any payment could be read by pairing its id with one of the caller's *own* accounts.

### Stage 4 — Outbox → Kafka
`proto` module and `Money` representation, protobuf serdes, outbox relay poller with backoff and
parking, topic creation scripts.
**Exit:** one payment ⇒ exactly one event. **Kafka-down test passes**: payment still succeeds,
event delivered after recovery.

**Findings during implementation.**

1. **The service would not have started.** `@Scheduled(fixedDelayString =
   "${payment.outbox.poll-interval}")` re-reads the property with its own parser, which accepts
   milliseconds or ISO-8601 but not the `500ms` in `application.yaml` — so the relay bean failed to
   initialise and took the application with it. Every other test disabled the relay, so only the
   Spring end-to-end test saw it. The relay is now scheduled programmatically from the
   already-bound `Duration`: one key had been read by two parsers with different rules, and now has
   one.
2. **Every outage would have been parked.** `KafkaTemplate` wraps producer failures in Spring's
   `KafkaProducerException`, which is not a Kafka `RetriableException`, so classifying the exception
   as received treated an unreachable broker as permanent and parked after five attempts. The cause
   chain is now unwrapped first. Confirmed by reintroducing the bug: the never-parks test fails.
3. **Retriable failures must never park.** The plan said "after N attempts, park"; applied to every
   failure, a long outage would park the entire backlog and turn a self-healing incident into a
   manual replay. Parking is now for non-retriable errors only, with a dedicated `parked_at` column
   in migration V4 rather than an edit to the already-applied V2.
4. **The backlog gauge froze when the relay stalled** — see §17.3.
5. **Producer defaults are wrong for a producer inside a transaction.** `max.block.ms` defaults to
   60s and `delivery.timeout.ms` to 120s, during which the relay holds outbox row locks. Both are
   now a few seconds, below the relay's own send timeout, so an absent broker fails fast.

The stage's exit criteria hold: one payment yields exactly one event; 32 concurrent requests
sharing a key yield one payment and one event, the losers' outbox rows rolling back with their
debits; with the broker unreachable the payment still completes, its event is retried with backoff
and delivered on recovery; and two relays draining 200 events concurrently publish each exactly
once.

### Stage 5 — Notification module
Kafka Streams dedup topology with a bounded state store, EOS v2, punctuator-based purge, DLT,
notification sink (logged; note where a real provider call would go).
**Exit:** duplicate input ⇒ single notification, proven via `TopologyTestDriver` and an embedded
Kafka test.

**Findings during implementation.**

1. **The notification side effect must not run inside the topology.** The plan put the sink in
   the Streams app. Under `exactly_once_v2` an aborted transaction is re-run from the last
   commit, so a processor that sends an email sends it again, and no Kafka transaction can take
   it back. That is the caveat §6 states, and putting the sink in a processor would have
   reintroduced the exact duplicate deduplication exists to remove. The topology therefore stops at
   `notification-events`, and a separate listener reading with `read_committed` performs the
   delivery: retried with exponential backoff, then dead-lettered to a new
   `notification-events-dlt` topic. The residual duplicate — a crash between sending and
   committing the offset — is the one no broker guarantee removes, and is why the sender contract
   asks for the payment id as the provider's idempotency key.
2. **Deduplication silently depends on the record key.** The store is per partition, so a
   duplicate is only caught if it reaches the same partition as the original — guaranteed by the
   account-id key, and by nothing else. A producer keying by payment id, or not keying at all,
   would scatter copies across partitions and get them notified more than once with no error
   anywhere. Records whose key is not the payload's account id are now dead-lettered as
   `key_mismatch`, turning a silent correctness loss into a visible one.
3. **`LogAndContinueExceptionHandler` drops poison messages.** The planned configuration would
   skip an undecodable record with a log line and nothing else. Values are now read as raw bytes
   and decoded inside the processor, so an undecodable record reaches `payment-events-dlt` with
   its original bytes and headers, plus the reason and its original coordinates, ready to be
   inspected and replayed. Events that decode but cannot be notified — missing fields, an unknown
   status from a newer producer — go the same way rather than being notified with a guessed
   message.
4. **The pinned Kafka Streams version failed at runtime.** The version catalog pinned
   `kafka-streams` to 3.8.0, but Spring Boot's dependency management resolves `kafka-clients` to
   3.7.1, the version Spring Kafka 3.2 is built against — Gradle reported no conflict, and the
   first `TopologyTestDriver` failed with `NoClassDefFoundError`. Streams is now unversioned in the
   catalog so Boot keeps both aligned. A 3.7 client against the 3.8 broker in `compose.yaml` is
   fully supported.
5. **Counters in a processor are not transactional.** They are incremented in the JVM, outside
   the EOS transaction, so a batch re-processed after an abort is counted twice. The counters are
   exact in steady state and may over-count around a crash or rebalance; the notifications
   themselves are unaffected. Stated in the metrics class rather than left to be discovered.
6. **A new deployment notifies the retained backlog.** A new consumer group starts from the
   oldest offset, so the first start notifies every payment still on the topic — up to seven
   days of them. Seen live during verification. This is deliberate: starting from the newest
   offset instead would silently skip notifications whenever committed offsets are lost. A real
   first rollout would either accept this or skip events older than a cut-off, which is a
   product decision rather than a technical one.
7. **A thread failure must be visible to the platform.** An exception escaping the processor is
   a bug or an unwritable output, not a bad record — those are dead-lettered — so retrying in
   place fails the same way. The uncaught-exception handler shuts the client down, and a health
   indicator reporting the Streams state is part of the liveness group, so the instance gets
   replaced. A broker outage does not trip it: the client stays `RUNNING` and retries.

**Purging scales linearly with the store.** The punctuator scans the whole store hourly. Bounded
by the retention, that is around eight million small entries at a million payments a day, spread
across partitions. A windowed store, whose segments expire wholesale, would avoid the scan, at
the cost of a lookup across segments on every event. The plain store reads more clearly for the
exercise.

**Tests.** `TopologyTestDriver` covers deduplication, retention at and past its boundary, and
every dead-letter path, against a mocked wall clock. The planned embedded-Kafka test is instead
a Testcontainers broker, as in the payment service, with the full Spring context. It proves that
three copies of an event yield one committed notification and one delivery, that both
dead-letter topics work without blocking the partition, and that deduplication survives a
restart with the local state deleted. That last test only passes if the store is restored from
its changelog: disabling the changelog makes it fail, and so does removing the deduplication
check. Asserting that something happened *once* needs a point after which a second occurrence
would already be visible. Each scenario therefore ends with a marker event on the same partition,
and makes its assertions once the marker is delivered.

### Stage 6 — Component tests end to end
Separate `src/component` source set (mirroring the reference repo) spinning payment-service +
notification-service + Postgres + Kafka via Testcontainers.
**Exit:** `POST` → DB row → Kafka event → notification asserted end to end.

**How.** Both services run as the Docker images their `Dockerfile`s build, rather than as Spring
contexts inside the test JVM. In-process would have been faster, but both modules put an
`application.yaml` at the classpath root, so only one would load, and the result would not be
what is deployed. The source set no longer sees the main classes, which keeps the tests
black-box: they reach the system only over HTTP, Kafka and SQL, plus the delivery line the
notification sender logs, which is the side effect a real provider call would replace. Topics
come from the compose stack's own `create-topics.sh`, so drift between that script and the
services fails here.

**Findings during implementation.**

1. **The notification image could not run Kafka Streams.** It was built on
   `eclipse-temurin:21-jre-alpine`. RocksDB, which backs the deduplication store, ships a native
   library linked against glibc's `libstdc++`. On Alpine's musl it fails to load, every stream
   thread dies on its first store access, and the client goes to `ERROR`. Nothing earlier could
   have caught it: the unit, `TopologyTestDriver` and broker tests all run on the host JVM. Both
   images now use the glibc-based `eclipse-temurin:21-jre`. This one finding is the argument for
   testing the image rather than the jar.
2. **"Ready" did not mean "processing".** The notification service reported ready before Streams
   had started, so the broken image above passed every startup check, and the failure surfaced
   only as delivery timeouts in unrelated tests. Two changes follow. The Streams health indicator
   is now in the readiness group as well as liveness, so a rolling deployment of an image that
   cannot run halts at the first instance rather than replacing healthy ones. And the component
   stack waits for Streams to report `RUNNING` before any test starts. Reintroducing the Alpine
   image now fails at startup with the health response: `"kafkaStreams":{"status":"DOWN",
   "details":{"state":"ERROR"}}`.
3. **The catalog's Testcontainers version was not the one in use.** Spring Boot's dependency
   management silently resolved Testcontainers to 1.19.8 over the catalog's 1.20.2, and the
   cross-container Kafka listener this stage needs only exists in 1.20. Overriding Boot's
   `testcontainers.version` property makes the catalog authoritative again. This is the same
   failure mode as the Stage 5 Kafka Streams pin, in the opposite direction.
4. **The tests did not rerun when an image changed.** `componentTest` declared the jars as inputs
   but not the `Dockerfile`s, so Gradle reported the task up to date after a base-image change. It
   surfaced when a mutation check reported success in one second. The Dockerfiles,
   `.dockerignore` files and topic script are now declared inputs.
5. **An outage is simulated by pausing the broker, not stopping it.** Stopping a container
   releases its mapped port and network identity, so it cannot come back at the same address.
   Pausing keeps both, and connections hang rather than being refused. That is the harder case,
   because clients block on timeouts instead of failing fast, and it is what a network partition
   looks like. The test asserts the payment returns in under two seconds while the relay records
   failed attempts and parks nothing.

Testcontainers' HTTP wait strategy with a response-body predicate never matched, although the
endpoint returned the expected body when probed by hand. Rather than depend on it, the stack
polls the liveness endpoint itself and fails with the last response it saw.

### Stage 7 — Polish
README design-decision write-up (locking strategy, ORM vs SQL, outbox vs CDC, key design, why
Streams, what HA requires, explicit security exclusions), GitHub Actions CI, Actuator, final
review pass.

---

## 15. Decisions taken

| Decision | Choice | Rationale |
|---|---|---|
| **Payment semantics** | Outbound payment to an external beneficiary | A single `ACCOUNT_ID` plus beneficiary details is then *correct* rather than a simplification. Internal account-to-account transfer is **explicitly out of scope** — see below. |
| **Notification pipeline** | Single `payment-events` topic, Kafka Streams dedup | No dual write; the outbox makes the event atomic with the debit. Still exercises Streams, a state store, EOS v2 and `TopologyTestDriver`. |
| **Locking** | Pessimistic `SELECT ... FOR UPDATE` | Correct across replicas, serializes per account, parallel across accounts. Limitations documented below. |

### Out of scope: internal account-to-account transfer

Only **outbound** payments are implemented: funds leave a held account toward an external
beneficiary identified by name and IBAN. The counterparty is outside the system, so no credit
side is recorded and `PAYMENT` is deliberately one-sided.

An internal transfer would be a materially different feature, not an increment:

- **Two-sided journalling.** A transfer is a balanced pair — debit one account, credit another,
  summing to zero. Either two `PAYMENT` rows sharing a transfer id, or a
  `(debit_account_id, credit_account_id)` pair. The current one-sided row cannot express it.
- **Deadlock avoidance becomes mandatory.** Two concurrent transfers A→B and B→A each lock their
  first account and then block on the other's, and Postgres resolves it by killing one
  transaction. Correctness requires locking accounts in a deterministic order (ascending id) in
  every code path — a global invariant, easy to break silently.
- **Cross-currency.** Two accounts may hold different currencies, pulling in FX rates, rate
  sourcing, rounding policy and a spread. Out of scope here (outbound payments require the
  payment currency to equal the account currency).
- **Atomicity across both legs.** Both legs must commit or neither; partial application creates
  or destroys money.

This is the natural next feature and the schema does not block it — `PAYMENT` would gain a
nullable `credit_account_id` and a `transfer_id`.

### Documented limitations of pessimistic `SELECT ... FOR UPDATE`

Chosen for correctness and clarity, but the trade-offs are real and belong in the README:

1. **Serialized throughput per account.** All payments on one account execute one at a time.
   Throughput on a single hot account is bounded by transaction duration (~ms here, since the
   transaction touches only Postgres). Different accounts proceed fully in parallel, so this
   only bites on a genuinely hot account.
2. **Lock waits consume a connection.** Each waiter holds a HikariCP connection while blocked.
   Without a bound, a burst on one account can exhaust the pool and stall *unrelated* accounts.
   Mitigated by a short `lock_timeout` mapped to `503` + `Retry-After`, and by keeping the pool
   larger than the expected concurrent-waiter count.
3. **Long transactions amplify the cost.** The lock is held until commit, so the transaction
   must contain no network I/O. This is a reason the Kafka publish is *outside* it — the outbox
   row is written transactionally and published by the relay afterwards.
4. **No fairness guarantee.** Postgres grants row locks approximately FIFO but does not
   contractually guarantee it. The guarantee is *serialization*, not arrival ordering (§12 #8).
5. **Deadlock risk with multi-row locking.** Not an issue for outbound payments, which lock a
   single account row — but it becomes a hard requirement the moment internal transfers are
   added.
6. **Optimistic locking would invert these trade-offs:** no lock held and no connection pinned,
   at the cost of retry logic and wasted work under contention. Appropriate if a single account
   ever became a genuine throughput bottleneck.

---

## 16. High availability in a production deployment

The exercise runs a single service instance against a single Postgres and a single Kafka
broker. That is a *development* topology, not a highly available one. This section sets out
what production would require, and — more usefully — how the system would actually behave
when each component fails.

### 16.1 What the current design already gets right

Three properties are prerequisites for HA, and were chosen deliberately rather than
discovered later:

1. **The service is stateless.** No in-JVM locks, no session affinity, no in-memory
   deduplication. Every piece of coordination lives in Postgres. This is the single most
   important HA decision in the codebase: had concurrency been handled with a
   `ConcurrentHashMap<accountId, Lock>` — the obvious first instinct — the service would be
   *correct only at one replica*, and scaling out would silently reintroduce double spending.
2. **Invariants are enforced by the database, not the application.** `CHECK (balance >= 0)`
   and `UNIQUE (account_id, idempotency_key)` hold no matter how many replicas run, in what
   order they execute, or which of them is mid-deploy with older code.
3. **Every payment touches exactly one account.** There are no cross-account transactions, so
   the account id is a natural shard key and the write path is *already* horizontally
   shardable with no distributed transaction. This falls out of excluding internal transfers —
   those would need two-phase commit across shards. Worth noting that the scope decision in
   §15 bought a scalability property, not just a simpler schema.

Idempotency also turns out to matter far beyond client retries: **it is what makes failover
recoverable.** When a primary dies mid-transaction the client sees an error and cannot know
whether the payment committed. Retrying with the same `Idempotency-Key` is safe, and resolves
to the original outcome either way. Without it, every failover would carry a double-spend risk.

### 16.2 Service replicas

**Target: ≥ 3 replicas spread across ≥ 3 availability zones.**

Three rather than two, because a rolling deploy voluntarily removes one: at two replicas a
deploy leaves no headroom for a concurrent failure, and losing one instance halves capacity.
At three across zones, a zone outage costs a third.

| Concern | Setting |
|---|---|
| Spread | `topologySpreadConstraints` on `topology.kubernetes.io/zone` |
| Voluntary disruption | `PodDisruptionBudget` with `minAvailable: 2` |
| Readiness | `/actuator/health/readiness` — fails when Postgres is unreachable, so the instance leaves the load balancer |
| Liveness | `/actuator/health/liveness` — must **not** depend on Postgres |
| Shutdown | `server.shutdown=graceful` plus `terminationGracePeriodSeconds` above the longest request |

The split between the two probes is not cosmetic. If liveness depended on Postgres, a database
outage would make Kubernetes kill and restart every replica simultaneously — a restart storm
that adds connection-stampede load exactly when the database is already struggling, and
lengthens the outage. The correct behaviour is to stop accepting traffic while staying alive,
ready to serve the moment the database returns.

**The connection-pool trap.** Pool size multiplies by replica count: 20 connections × 12
replicas is 240 connections, against a default Postgres `max_connections` of 100. Scaling out
to improve availability would take the database down. Production needs either a reduced
per-replica pool or **PgBouncer in transaction pooling mode**.

Transaction pooling is compatible with this design precisely because it keeps no session state
across transactions — but it has a direct consequence for the lock timeout: a session-level
`SET lock_timeout` does not survive, since the backend is handed to another client between
transactions. The timeout must be applied **inside** the transaction as `SET LOCAL
lock_timeout`, which is how the `payment.lock-timeout` property should be implemented in
Stage 2.

### 16.3 PostgreSQL

"Distributed database" covers two architectures with materially different consequences, and the
distinction matters here.

**Primary with synchronous standbys** — Patroni/repmgr, or managed (RDS Multi-AZ, Cloud SQL HA,
Aurora). A single writer, so `SELECT ... FOR UPDATE` keeps *exactly* the semantics this design
relies on. Failover is 10–60 seconds depending on the stack.

```
synchronous_commit = on
synchronous_standby_names = 'ANY 1 (standby_a, standby_b)'
```

This gives **RPO = 0**: a payment acknowledged to the client survives the loss of the primary,
because commit does not return until at least one standby has the WAL record. That is not free
— every commit now includes a network round trip, which shows up in p99 latency. For a payment
system it is the right trade: asynchronous replication means a window in which a customer was
told their payment succeeded and the record no longer exists. Money cannot be eventually
consistent with itself.

**Distributed SQL** — CockroachDB, YugabyteDB, Spanner. Scales writes horizontally and
survives node loss transparently, but **migrating is not a configuration change**:

- `SELECT ... FOR UPDATE` becomes a distributed lock, paying consensus latency per payment —
  single-digit milliseconds becomes tens, more across regions.
- Serializable isolation surfaces **retryable transaction conflicts** (SQLSTATE `40001`). The
  application *must* retry them. The current code would surface them as `500`s, so a retry
  wrapper around the payment transaction is mandatory before such a move.

For this workload a single synchronously-replicated primary is the right answer, and if one
primary ever saturates, **sharding by account id** is the escape hatch — available precisely
because no transaction spans accounts.

**Client-side requirements for failover to actually work:**

```
jdbc:postgresql://pg-a:5432,pg-b:5432,pg-c:5432/payment?targetServerType=primary
```

plus Hikari validation to evict connections to the demoted node. Without this, replicas keep
handing out connections to a server that is no longer the primary and every write fails.

### 16.4 Kafka

**Target: ≥ 3 brokers, one per availability zone.**

| Setting | Production value | Why |
|---|---|---|
| `replication.factor` | `3` | Survives one broker loss with a replica to spare |
| `min.insync.replicas` | `2` | A write is durable on two brokers before acknowledgement |
| `acks` (producer) | `all` | Acknowledge only once `min.insync.replicas` have it |
| `unclean.leader.election.enable` | `false` | **Critical** |
| `broker.rack` | per-AZ identifier | Makes RF=3 mean three zones, not three machines in one rack |
| `transaction.state.log.replication.factor` | `3` | Defaults to 1 — a production footgun |
| Streams `replication.factor` | `3` | Applies to changelog topics; also defaults to 1 |
| Streams `num.standby.replicas` | `≥ 1` | See below |

`unclean.leader.election.enable=false` deserves emphasis: set to `true`, an out-of-sync replica
may be elected leader, **silently discarding committed events**. For payment notifications that
means a customer never hears about a payment that definitely happened. The correct failure mode
is to make the partition unavailable rather than to lose data.

The RF=3 / ISR=2 combination tolerates exactly one broker loss while still accepting writes.
Lose two and the partition goes read-only — producers receive `NOT_ENOUGH_REPLICAS`. That is
the intended behaviour: refuse data that cannot be stored durably rather than accept it and
hope.

**Partition count must be over-provisioned from the start**, for two reasons that are easy to
discover too late:

1. Partition count caps consumer parallelism. A consumer group can run at most one instance per
   partition, so 3 partitions means the notification service cannot usefully scale past 3
   instances no matter how far behind it falls.
2. Partitions can be increased but **never decreased**, and increasing them **breaks key→
   partition affinity**. Keys rehash, so an account's existing events stay on the old partition
   while new ones land elsewhere — which breaks the per-account ordering the key design exists
   to provide, permanently for the straddling window.

So `payment-events` should be created with 12–24 partitions rather than grown later. Rough
sizing: at least as many partitions as peak expected consumer instances, around one partition
per 10 MB/s of target throughput, keeping the per-broker total in the low thousands.

**Streams state recovery is the notification service's main HA lever.** The deduplication store
is backed by a changelog topic. Without standby replicas, an instance failure means the
replacement replays that changelog from scratch before processing resumes — minutes of
notification delay proportional to state size. `num.standby.replicas: 1` keeps a warm copy on
another instance and turns that into seconds. Pair it with static group membership
(`group.instance.id`) so rolling restarts do not trigger full rebalances.

### 16.5 The outbox relay across replicas

The relay needs **no leader election and no coordination**: `SELECT ... FOR UPDATE SKIP LOCKED`
means each replica claims a disjoint batch, and a replica that dies mid-batch simply releases
its locks when the connection closes, so another picks the rows up on the next poll. This is an
unusually clean horizontal-scaling story and it comes from one SQL clause.

**One honest caveat.** With multiple relay replicas polling concurrently, two replicas can
claim two payments for the *same account* at the same time and publish them in either order. So
although events are keyed by account id — which guarantees Kafka *stores* them on one partition
— the published order may not match database commit order.

For the notification use case this is harmless: the consumer deduplicates by payment id and
does not care about order. The design accepts it deliberately. But any future order-sensitive
consumer (a running-balance projection, say) would need the relay sharded by key, e.g.:

```sql
WHERE published_at IS NULL
  AND next_attempt_at <= now()
  AND abs(hashtext(partition_key)) % :relay_count = :relay_index
```

which gives one relay sole ownership of a given account's events, restoring order at the cost
of coordinating shard assignment. Stating the limitation is more useful than implying an
ordering guarantee the relay does not provide.

### 16.6 Behaviour under failure

This is what "highly available" actually means in operation:

| Failure | Observed behaviour | Recovery |
|---|---|---|
| **One service replica** | Removed from the load balancer. In-flight requests fail; clients retry with the same `Idempotency-Key`, so no double spend. Capacity drops by 1/N. | Automatic; orchestrator reschedules |
| **Postgres primary** | In-flight transactions abort. API returns `503` for 10–60s. Committed payments intact (RPO 0). Retries with the same key resolve to the original outcome. | Automatic promotion |
| **One Kafka broker** | Producers continue (ISR still ≥ 2). Brief `NOT_LEADER_FOR_PARTITION` retries, handled inside the client. | Automatic leader election |
| **Two Kafka brokers** | Publishing stops (`NOT_ENOUGH_REPLICAS`). Outbox rows accumulate, `attempts` increments with backoff. **Payments keep succeeding and returning `201`.** Notifications are delayed, not lost. | Relay drains the backlog automatically |
| **Kafka entirely down** | As above. The one SLI that detects this is outbox depth — the API looks perfectly healthy. | Relay drains on recovery |
| **Notification service down** | Consumer lag grows; events sit in the topic for their 7-day retention. Zero payment impact. | Restart; state restored from changelog (seconds with a standby, minutes without) |
| **Relay replica mid-batch** | Row locks release on connection close. Another replica reclaims them. If it died after publishing but before marking, the event republishes and the consumer deduplicates. | Automatic |
| **One availability zone** | A third of replicas and one broker lost; Postgres fails over if the primary was there. Degraded capacity, no data loss. | Automatic |
| **Whole region** | Not covered by multi-AZ HA — see §16.8. | Manual DR |

The Kafka rows are the point worth dwelling on. **A total Kafka outage does not stop payments**,
because the outbox makes event publication asynchronous and recoverable. The system degrades
(notifications arrive late) instead of failing (payments rejected). That is why Kafka is
deliberately excluded from the readiness probe, and it is the single most valuable
fault-tolerance property in the design.

**What must be alerted on** for this to be real rather than theoretical:

- **Outbox depth and age of the oldest unpublished row** — the primary SLI. A stuck relay is
  completely invisible from the API: payments succeed, notifications silently stop.
- Consumer group lag on `payment-events`.
- Postgres replication lag; count of synchronous standbys in sync.
- Kafka under-replicated partitions and ISR shrink events.
- Ready replica count against desired.
- Rate of `503`s from lock timeouts — rising values indicate contention on hot accounts.

### 16.7 Configuration: development versus production

| | This repository | Production |
|---|---|---|
| Service replicas | 1 | ≥ 3, across ≥ 3 AZs |
| Postgres | 1 container | Primary + ≥ 2 synchronous standbys, multi-AZ |
| `synchronous_commit` | `on` (no standby) | `on` with `synchronous_standby_names` |
| Connection pooling | Hikari only | Hikari + PgBouncer (transaction mode) |
| Kafka brokers | 1 | ≥ 3, one per AZ, `broker.rack` set |
| Replication factor | 1 | 3 |
| `min.insync.replicas` | 1 (default) | 2 |
| `unclean.leader.election` | default | `false` |
| Partitions (`payment-events`) | 3 | 12–24 |
| Streams standby replicas | 0 | ≥ 1 |
| Kafka security | PLAINTEXT | mTLS + SASL, ACLs per principal |
| Schema management | Flyway on startup | Separate migration step before rollout |
| Schema compatibility | none | Schema Registry, backward-compatible |

Two of these are more than tuning. **Flyway on application startup** is convenient locally but
wrong for a fleet: a failed migration during a rolling deploy can prevent every replica from
starting, converting a bad migration into a total outage. Production runs migrations as a
discrete, gated step. And **migrations must be backward compatible with the running version**,
because a rolling deploy means old and new code query the same schema simultaneously — which
makes column drops and renames a multi-release expand/contract sequence rather than a single
change.

### 16.8 What high availability does not provide

Worth stating plainly, because "HA" is often read as "nothing can go wrong":

- **Multi-AZ is not multi-region.** Surviving a region loss needs cross-region replication,
  which is asynchronous over that distance and therefore **RPO > 0** — a bounded window of
  acknowledged payments that may not survive. For a payment system that is not an engineering
  detail but a documented reconciliation procedure. Active-active multi-region is not viable
  with `SELECT FOR UPDATE` at all, since the lock would pay inter-region latency; it requires
  sharding accounts to a home region.
- **HA does not protect against bad deploys or data corruption**, which it faithfully
  replicates to every replica. That needs backups with point-in-time recovery — and the
  append-only `payment` journal is what makes state reconstructable after a logical error,
  which is a large part of why the journal is append-only.
- **HA is not capacity.** Three replicas sized for peak load have no headroom when one is lost.
  Replica count must account for the failure case, not just the steady state.

---

## 17. Observability

§16.6 identified outbox depth as the primary service level indicator, which is only useful if
something actually measures it. This section covers what is instrumented and why those
particular things.

### 17.1 What is exposed

Micrometer is wired to a Prometheus registry, served by Actuator:

| Endpoint | Purpose |
|---|---|
| `/actuator/prometheus` | All meters in Prometheus text exposition format |
| `/actuator/metrics` | The same meters as JSON, convenient for ad-hoc inspection |
| `/actuator/health` | Component health, with `liveness` and `readiness` groups |

Nothing scrapes the endpoint in this setup — it is exposed so the metrics can be read directly.
Every meter carries an `application` tag so a single Prometheus could serve both services.

Alongside the application metrics, the Spring Boot defaults provide HTTP server timings
(`http_server_requests_seconds`, tagged by URI, method and status), HikariCP pool state,
JVM memory and GC, and Kafka client metrics.

### 17.2 Metric catalogue

| Metric | Type | Tags | Purpose |
|---|---|---|---|
| `payment_attempts_total` | counter | `outcome` | Attempts by terminal outcome |
| `payment_amount` | summary | `currency` | Value of completed payments |
| `payment_execution_seconds` | timer | — | Transactional debit, lock acquisition to commit |
| `payment_lock_wait_seconds` | timer | — | Time waiting for the per-account row lock |
| `payment_outbox_pending` | gauge | — | Rows awaiting publication |
| `payment_outbox_oldest_pending_age_seconds` | gauge | — | Age of the oldest unpublished row |
| `payment_outbox_relay_last_poll_age_seconds` | gauge | — | Time since the relay last completed a poll |
| `payment_outbox_published_total` | counter | — | Rows published successfully |
| `payment_outbox_publish_failures_total` | counter | — | Failed publication attempts |
| `payment_outbox_parked_total` | counter | — | Rows parked after exhausting retries |

The `outcome` tag takes its values from the `PaymentOutcome` enum: `completed`,
`insufficient_funds`, `currency_mismatch`, `account_not_found`, `account_not_owned`,
`idempotent_replay`, `idempotency_key_reused`, `lock_timeout`, `validation_failed`.

The notification service registers its own, on `:8081`:

| Metric | Type | Tags | Purpose |
|---|---|---|---|
| `notification_events_total` | counter | `outcome` | Payment events by what deduplication did with them |
| `notification_deliveries_total` | counter | `outcome` | Delivery attempts by outcome |
| `notification_deduplication_purged_total` | counter | — | Payment ids purged after their retention |

Event outcomes are `notified`, `duplicate_suppressed` and `dead_lettered`; delivery outcomes are
`sent`, `failed` and `dead_lettered`. Kafka Streams client metrics, consumer lag among them, are
bound to Micrometer by Spring. `duplicate_suppressed` is worth singling out: it is the direct,
observable evidence that the deduplication requirement is working, rather than something
inferred from the absence of complaints.

### 17.3 Instrumentation decisions

**Metric definitions are centralised in one class.** `PaymentMetrics` holds every meter rather
than scattering `registry.counter(...)` calls through the code. Metric names are a published
interface — dashboards, alert rules and SLOs reference them, so renaming one is a breaking
change for whoever is on call at the time. One class keeps that interface reviewable in a diff.

**Every outcome counter is registered at zero on startup.** This is not cosmetic. A counter that
has never been incremented is *absent entirely* from a Prometheus scrape, so an alert rule like

```promql
rate(payment_attempts_total{outcome="insufficient_funds"}[5m]) > 10
```

would evaluate against a non-existent series until the first such payment — precisely the
moment the rule needs to already be working. Pre-registering every enum value means every
series exists from startup. A unit test enforces this, because it is exactly the kind of
property that a later refactor silently removes.

**Lock wait is timed separately from execution.** Both are needed to tell two very different
incidents apart: both rising means the database itself is slow, whereas only the wait rising
means contention on a hot account. Conflating them into one timer hides that distinction at the
point in an incident where it is the only question worth answering.

**Backlog gauges are fed by the relay, not queried per scrape.** A naive gauge would run
`SELECT count(*)` on every scrape, adding database load proportional to the number of scrapers
and the size of the table. Instead the relay — which is already querying the outbox every 500ms
— refreshes holders that the gauges read, so the scrape costs nothing.

That design has a trap, found in Stage 4: a gauge fed by the relay freezes when the relay stalls,
which is precisely the failure it exists to detect. The age gauge therefore holds the oldest row's
*creation timestamp* and computes the age at scrape time, so it keeps growing without updates. It
cannot cover a relay that died on an empty backlog — its last observation was "nothing pending" —
so a second gauge, time since the last completed poll, does; it is not advanced when a poll fails,
so a relay that keeps failing ages too.

**Amounts are tagged by currency, never aggregated across them.** A single
`payment_amount_sum` spanning CHF and EUR is a number with no meaning. Cardinality is safe
because the tag is bounded by ISO 4217, and in practice by the currencies the accounts hold.

**Histogram buckets are declared explicitly** rather than using
`percentiles-histogram: true`. The automatic option emits roughly seventy buckets per timer
spanning up to 30 seconds; for an operation expected to finish in single-digit milliseconds
that is both a large number of wasted series and resolution concentrated in a range that never
occurs. The declared boundaries (1ms to 3s, ending at the lock timeout beyond which a request
is rejected anyway) cut the payment metric series from roughly 160 to 41.

Buckets are published *in addition to* client-side percentiles, because the two answer
different questions. Client-side percentiles are correct for one instance but **cannot be
averaged across replicas** — the mean of three instances' p99 values is not the fleet p99.
Cumulative buckets are mergeable, so `histogram_quantile()` computes a true fleet-wide quantile
server-side. With multiple replicas, only the bucket form gives a meaningful answer.

### 17.4 What to alert on

Roughly in order of value:

| Signal | Condition | Why it matters |
|---|---|---|
| `payment_outbox_oldest_pending_age_seconds` | > 60s sustained | **The highest-value alert.** A stalled relay is invisible from the API: payments return `201` and notifications silently stop. Age beats depth — a deep but draining backlog is healthy, a shallow static one is not |
| `payment_outbox_relay_last_poll_age_seconds` | > 30s | The relay is not running, or every poll is failing — including on an empty backlog, where the age gauge cannot see it |
| `payment_outbox_parked_total` | any increase | A poison event needing human inspection |
| `payment_attempts_total{outcome="lock_timeout"}` | rising rate | Contention on a hot account; capacity problem forming |
| `payment_lock_wait_seconds` p99 | rising while execution flat | Contention, as distinct from database slowness |
| `payment_attempts_total{outcome="account_not_owned"}` | any sustained rate | Legitimate clients do not address other users' accounts; this is a client bug or probing |
| `hikaricp_connections_pending` | > 0 sustained | Pool exhaustion, often downstream of lock contention (§16.2) |
| Consumer group lag on `payment-events` | growing | Notification service falling behind |
| `notification_events_total{outcome="dead_lettered"}` | any increase | An event no one was notified about; the record waits in `payment-events-dlt` |
| `notification_deliveries_total{outcome="dead_lettered"}` | any increase | A notification that exhausted its retries; the provider may be down |

### 17.5 Logs and traces

The payment UUID is placed in the MDC and rendered in the log pattern, so one payment can be
followed from the API through the outbox to the published event. In production this would be
JSON-encoded rather than the human-readable pattern used locally, so a log backend can index
the fields.

**Distributed tracing is not implemented.** Two services and a Kafka hop is where traces start
to earn their cost, and the extension is small: `micrometer-tracing-bridge-otel` plus an OTLP
exporter, with trace context propagated through Kafka headers so a payment's REST request,
outbox publication and notification appear as one trace. Worth noting that this is the piece
that would most improve debuggability, because the current correlation id requires knowing
which service to look in.

### 17.6 Production: shipping metrics onward

Exposing `/actuator/prometheus` is deliberately where this stops. Production would scrape it,
and for Datadog specifically there are three routes:

1. **Datadog Agent with the OpenMetrics check** (recommended) — annotate the pod, the agent
   scrapes the existing endpoint. The application stays vendor-neutral, with no new dependency
   and no credentials in the service.
2. **OpenTelemetry Collector** — scrape with the Prometheus receiver, fan out to Datadog and
   anywhere else. Best when metrics must reach more than one backend, or to keep the exit
   vendor-agnostic.
3. **`micrometer-registry-datadog`** — push directly from the application. Fewest moving parts,
   but it couples the service to a vendor, puts an API key in its configuration, and makes the
   app responsible for delivery and buffering.

Option 1 or 2, because a metric endpoint is a far better integration seam than a push client:
changing observability vendors then touches deployment configuration rather than application
code. The same applies to the `management.metrics.tags` entry already in place — `application`
becomes a Datadog tag with no further work.

Also deliberately out of scope: dashboards, alert rules as code, SLO definitions, and trace
sampling policy. The metric *contract* in §17.2 is the part that must be right first, since
everything downstream references it by name.
