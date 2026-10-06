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

- **HTTP contract** via RFC 7807 `ProblemDetail` (built into Spring 6) and a
  `@RestControllerAdvice`:
  | Condition | Status |
  |---|---|
  | Malformed / invalid body, non-positive amount | `400` |
  | Unknown account | `404` |
  | Insufficient funds | `409` |
  | Idempotency key reused with a different payload | `409` |
  | Currency mismatch | `422` |
  | Lock timeout / contention | `503` + `Retry-After` |
- **Account ownership check.** `userId` in the path is only meaningful if the service verifies
  the account belongs to that user. Without it the path is a trivial IDOR. This is business
  logic, not auth — it should exist and be tested even with auth out of scope.
- **`lock_timeout` / `statement_timeout`** so a stuck lock doesn't exhaust the connection pool.
  Map the timeout to `503`.
- **Consumer-side DLT:** Spring Kafka `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`;
  in Streams, a `DeserializationExceptionHandler` (`LogAndContinue`) and a production exception
  handler.
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
outbox claim. Testcontainers integration tests including **the N-thread single-account
concurrency test**.
**Exit:** concurrency test green; balance never negative; exactly one payment per idempotency key.

### Stage 3 — REST API
DTOs and mappers, controller, `@RestControllerAdvice` + `ProblemDetail`, idempotency handling,
account-ownership check, springdoc OpenAPI.
**Exit:** documented API, `@WebMvcTest` coverage of every error path, Swagger UI live.

### Stage 4 — Outbox → Kafka
`proto` module and `Money` representation, protobuf serdes, outbox relay poller with backoff and
parking, topic creation scripts.
**Exit:** one payment ⇒ exactly one event. **Kafka-down test passes**: payment still succeeds,
event delivered after recovery.

### Stage 5 — Notification module
Kafka Streams dedup topology with a bounded state store, EOS v2, punctuator-based purge, DLT,
notification sink (logged; note where a real provider call would go).
**Exit:** duplicate input ⇒ single notification, proven via `TopologyTestDriver` and an embedded
Kafka test.

### Stage 6 — Component tests end to end
Separate `src/component` source set (mirroring the reference repo) spinning payment-service +
notification-service + Postgres + Kafka via Testcontainers.
**Exit:** `POST` → DB row → Kafka event → notification asserted end to end.

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
