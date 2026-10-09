# Payment Service

A payment service exposing a REST API to check an account balance and submit outbound
payments, with the transaction journal in PostgreSQL and asynchronous sender notification
over Kafka.

Built for an assignment whose stated focus is **high availability, transactional processing
and error handling**. The design rationale — including the trade-offs rejected along the way —
is in [`docs/DESIGN_REVIEW.md`](docs/DESIGN_REVIEW.md).

## Status

Implemented in stages; each stage is independently buildable and green.

| Stage | Scope | State |
|---|---|---|
| 0 | Build skeleton, local stack, schema migrations | ✅ Done |
| 1 | Domain model, repository contract, in-memory implementation | ✅ Done |
| 2 | PostgreSQL implementation, row locking, concurrency tests | ✅ Done |
| 3 | REST API, error contract, OpenAPI | ✅ Done |
| 4 | Outbox relay → Kafka | ✅ Done |
| 5 | Notification service (Kafka Streams deduplication) | ✅ Done |
| 6 | End-to-end component tests | ✅ Done |
| 7 | Documentation and CI | ⏳ Next |

## Modules

| Module | Responsibility |
|---|---|
| `proto` | Protobuf definitions of the Kafka event contract, shared by both services |
| `payment-service` | REST API, payment execution, transaction journal, outbox relay |
| `notification-service` | Kafka Streams deduplication of payment events, and delivery of the resulting notifications |

## Running locally

Requires Docker and a JDK 21 toolchain (Gradle provisions the toolchain if absent).

```bash
./gradlew :payment-service:bootRun        # REST API on :8080
./gradlew :notification-service:bootRun   # notifications, actuator on :8081
```

`spring-boot-docker-compose` starts the [`compose.yaml`](compose.yaml) stack — PostgreSQL and
Kafka in KRaft mode — and wires the datasource and broker address into the application, so no
connection details are duplicated in configuration. Flyway applies the schema on startup and
seeds demo data.

To run the stack by hand instead:

```bash
docker compose up -d
SPRING_DOCKER_COMPOSE_ENABLED=false ./gradlew :payment-service:bootRun
```

### Verifying the stack

```bash
curl -s localhost:8080/actuator/health | jq

docker compose exec postgres psql -U payment -d payment -c '\dt'
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

## Building and testing

```bash
./gradlew build          # compile, unit tests, component tests, format check
./gradlew test           # unit tests only (no containers required)
./gradlew componentTest  # component tests (requires Docker)
./gradlew spotlessApply  # apply google-java-format
```

Component tests live in a dedicated `src/component` source set in `payment-service`. They run
the whole system as it would be deployed — both services as the Docker images their
`Dockerfile`s build, PostgreSQL, Kafka, and the topics created by the same
`docker/kafka/create-topics.sh` the compose stack uses — and reach it only over HTTP, Kafka and
SQL. The source set deliberately cannot see the services' classes. Each test follows a payment
through every hop to the notification service's delivery log:

| Scenario | What it proves |
|---|---|
| Happy path | `201` → journal and balance → outbox published → event on `payment-events` → notification → delivery |
| Retried request | Same `Idempotency-Key` twice: one debit, one event, one notification |
| Declined payment | `409 insufficient_funds`, balance unchanged, the payer told why |
| 20 concurrent payments on CHF 1000 | Exactly 10 succeed, the balance lands on 0.00, every attempt is notified |
| Kafka unreachable | The broker is paused mid-test: the payment still returns `201` in well under 2s, the relay retries without parking, and the notification arrives after recovery |

They take about a minute once the images are cached, and rerun whenever either service's code,
its `Dockerfile` or the topic script changes.

### Docker images

Each service has a `Dockerfile` that packages its boot jar on a JRE, running as a non-root user:

```bash
./gradlew :payment-service:bootJar :notification-service:bootJar
docker build -t payment-service payment-service
docker build -t notification-service notification-service
```

Both are configured entirely through the environment: `POSTGRES_URL`, `POSTGRES_USER` and
`POSTGRES_PASSWORD` for the payment service, `SPRING_KAFKA_BOOTSTRAP_SERVERS` for both.

## Design summary

The decisions below are argued in full, with their rejected alternatives, in
[`docs/DESIGN_REVIEW.md`](docs/DESIGN_REVIEW.md).

**Double spending is prevented in two independent places.** A client-supplied
`Idempotency-Key` header, backed by a unique index on `(account_id, idempotency_key)`, makes a
retried request replay its original outcome rather than debit twice — a server-generated id
cannot do this, because a retry would simply mint a new one. Separately, a
`CHECK (balance >= 0)` constraint on `account` means no code path can overdraft, regardless of
application-level bugs.

**Concurrent payments on one account are serialized by the database**, not by application
state. The transaction takes `SELECT ... FOR UPDATE` on the account row, so correctness holds
across multiple service replicas; an in-process lock would not. Payments on *different*
accounts proceed fully in parallel. The guarantee is serialization — no lost updates, no
overdraft — rather than arrival ordering, which is not observable over HTTP anyway.

**The event and the state change are atomic, via a transactional outbox.** The outbox row is
inserted in the same transaction as the debit, so there is no window in which Kafka holds an
event that PostgreSQL rolled back, nor a committed payment whose event was lost. A relay
publishes afterwards and claims rows with `FOR UPDATE SKIP LOCKED`, so replicas take disjoint
batches without coordinating. A consequence worth stating plainly: **payments keep succeeding
while Kafka is down**, and the relay catches up on recovery.

**Delivery is at-least-once, so the consumer deduplicates.** The notification service is a
Kafka Streams application keeping already-notified payment ids in a state store, with
`exactly_once_v2` making the store update and the output record commit together. The message
itself is sent by a separate `read_committed` listener, not from inside the topology, because
Streams re-runs an aborted transaction and would repeat any side effect in it. Note the precise
claim: this makes notification *effectively* once; the final side effect of delivering a
message to a person is at-least-once in any system.

**Events are keyed by account id, not payment id.** That keeps an account's events on one
partition and therefore ordered, and co-partitions them with the deduplication store. Topics
use `cleanup.policy=delete` with bounded retention: these are immutable facts, not state, and
compaction keyed on something unique per record would retain everything forever.

**Persistence uses `JdbcClient` with hand-written SQL** rather than an ORM. The task centres on
locking primitives — `FOR UPDATE`, conditional atomic updates, `SKIP LOCKED` — which read
clearly as SQL and are obscured by JPA, whose natural read-mutate-flush idiom is the exact race
being avoided. With four tables and no graph traversal, an ORM buys little.

## Data model

```
app_user ──1:N──> account ──1:N──> payment
```

One user holds many accounts; an account belongs to exactly one user; a payment debits exactly
one account. `payment` is an append-only journal — rows are never updated or deleted.

Payments are **outbound only**: funds leave a held account toward an external beneficiary, so
the single-sided `payment` row is correct rather than a simplification. Internal
account-to-account transfer is explicitly out of scope; `docs/DESIGN_REVIEW.md` explains why it
is a different feature rather than an increment (two-sided journalling, deterministic lock
ordering to avoid deadlock, and cross-currency FX).

Money is `NUMERIC(19,4)` in PostgreSQL and `BigDecimal` in Java, never a floating point type.
Timestamps are `TIMESTAMPTZ`.

Migrations are plain SQL under
[`payment-service/src/main/resources/db/migration`](payment-service/src/main/resources/db/migration)
and build the schema from scratch.

## REST API

Interactive documentation is served at **`/swagger-ui.html`**, and the OpenAPI document at
**`/v3/api-docs`**.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/users/{userId}/accounts/{accountId}/balance` | Current balance |
| `POST` | `/api/v1/users/{userId}/accounts/{accountId}/payments` | Submit a payment (`Idempotency-Key` header required) |
| `GET` | `/api/v1/users/{userId}/accounts/{accountId}/payments/{paymentId}` | Retrieve a payment, completed or declined |

`userId` in the path stands in for authentication, which is out of scope. In production it must
come from a validated JWT subject, never from the URL. The ownership check behind it is real:
an account is only ever read or debited for its owner, and an account belonging to someone else
returns the same `404` as one that does not exist.

### Trying it

With the service running, against the seeded demo account:

```bash
BASE=localhost:8080/api/v1/users/11111111-1111-1111-1111-111111111111/accounts/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1

curl -s $BASE/balance

curl -si -X POST $BASE/payments \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":{"value":"250.50","currency":"CHF"},
       "beneficiary":{"name":"Acme GmbH","iban":"CH93 0076 2011 6238 5295 7"},
       "reference":"Invoice 42"}'
```

Sending the same request again with the same key returns the original `201` and body, with
`Idempotent-Replayed: true`, and does not debit again.

### Responses

| Status | When | `code` |
|---|---|---|
| `201` | Payment completed | — |
| `400` | Malformed body, invalid field, missing or over-long `Idempotency-Key` | `validation_failed` |
| `404` | No such account for this user (or not theirs) | `account_not_found` |
| `409` | Declined for insufficient funds; the attempt is recorded, `paymentId` and `Location` identify it | `insufficient_funds` |
| `422` | Currency differs from the account's | `currency_mismatch` |
| `422` | Idempotency key already used for a *different* payment | `idempotency_key_reused` |
| `503` | Another payment on the account is in flight; retry after `Retry-After` with the same key | `lock_timeout` |

Errors are RFC 9457 problem details. Clients should branch on `code`, which is stable, rather
than on `detail`, which is prose. A replay returns the original status, so a client retrying after
a timeout receives exactly the response it missed — including a `409` if the original was declined.

Monetary values are decimal strings in responses (`"250.50"`), rendered at the currency's
minor-unit precision, so a client's JSON parser does not turn them into floating point. Requests
accept a string or a number; more than four decimal places is refused rather than rounded.

## Event publication

Payments publish a `PaymentEvent` (protobuf, defined in [`proto/`](proto)) to the
`payment-events` topic, for both completed and declined payments. Replays and rejected requests
publish nothing.

**Transactional outbox.** The event row is written in the same database transaction as the debit
and the journal entry, so an event exists if and only if its payment committed. A relay polls the
outbox and publishes to Kafka afterwards. The payment path never talks to Kafka, which is why a
broker outage delays notifications but cannot fail a payment.

**Relay.** Every replica runs one. Batches are claimed with `SELECT ... FOR UPDATE SKIP LOCKED`, so
concurrent relays take disjoint rows with no coordination. Delivery is at-least-once: if Kafka
acknowledges a batch and the commit marking it fails, it is sent again, and consumers deduplicate
on the `payment-id` header.

**Failures.** Retriable errors — an unreachable broker, a leader election — are retried with
exponential backoff capped at one minute, indefinitely and never parked. Otherwise a long outage
would park the whole backlog and need it replayed by hand. Non-retriable errors, such as a record
too large, are parked after `payment.outbox.max-attempts` so one poison message cannot block the
queue; parked rows have `parked_at` set and `last_error` explaining why.

**Records** are keyed by account id and carry `payment-id`, `event-type` and `content-type`
headers. To watch them with the values decoded (needs `brew install kcat protobuf`):

```bash
scripts/tail-events.sh       # every record so far, then exit
scripts/tail-events.sh -f    # keep following new records
```

Without those tools, Kafka's console consumer shows the key and headers, though the protobuf
value prints as raw bytes:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment-events --from-beginning \
  --property print.key=true --property print.headers=true
```

## Notifications

The notification service turns each payment event into one message to the payer, however many
times the event is delivered:

```
payment-events ──▶ Kafka Streams: deduplicate ──▶ notification-events ──▶ delivery listener ──▶ provider
                          │                                                     │
                          └──▶ payment-events-dlt                               └──▶ notification-events-dlt
```

**Deduplication.** A state store remembers every payment id already notified; a redelivered
event finds its id there and is dropped. Under `exactly_once_v2` the store write, the emitted
notification and the input offset commit in one transaction, so a crash cannot leave a
notification sent with its id unrecorded. The store is backed by a changelog topic, so it
survives losing an instance's disk, and it is bounded: a punctuator purges ids older than 8
days, longer than the input topic's 7-day retention, so no copy of an event can outlive its id.

The store is per partition, which is correct only because every copy of a payment's event is
keyed by the same account id and so lands on the same partition. An event keyed otherwise is
dead-lettered rather than checked against the wrong store.

**Delivery.** A listener reads `notification-events` with `read_committed`, so it sees only
notifications from committed transactions, and hands each to a `NotificationSender`. Here the
sender logs the message; it is where an email or push provider would be called, with the payment
id as the provider's idempotency key, since a crash between sending and committing the offset
sends again. Failed deliveries are retried with exponential backoff, then dead-lettered.

**Dead letters.** An event that cannot be notified — undecodable, missing a field, an unknown
status, or wrongly keyed — goes to `payment-events-dlt` with its original bytes and headers,
plus `dlt-reason`, `dlt-error` and its original topic, partition and offset. A notification
that fails every delivery attempt goes to `notification-events-dlt`. Neither blocks the
partition behind it.

To watch it work, run both services, submit a payment, and look for the `Notifying user` log
line in the notification service. `scripts/tail-events.sh -t notification-events` decodes the
deduplicated stream.

## High availability

This repository runs a development topology: one service instance, one PostgreSQL container,
one Kafka broker. The application-side design is what makes a highly available deployment
possible, and three properties were chosen with that in mind:

- **The service is stateless.** All coordination lives in PostgreSQL, so replicas need no
  affinity and no coordination. Handling concurrency with an in-process lock instead would have
  been correct at one replica and silently wrong at two.
- **Invariants are enforced by the database.** They hold regardless of replica count, execution
  order, or a rolling deploy running two code versions at once.
- **No transaction spans accounts**, so the account id is a natural shard key and the write path
  is already shardable with no distributed transaction.

Idempotency also makes failover recoverable: when a primary dies mid-transaction the client
cannot know whether the payment committed, and retrying with the same `Idempotency-Key` is safe
either way.

The most valuable fault-tolerance property is that **a total Kafka outage does not stop
payments**. The outbox makes publication asynchronous and recoverable, so the system degrades
(notifications arrive late) rather than failing (payments rejected). This is why Kafka is
deliberately excluded from the readiness probe — and it means outbox depth, not API health, is
the signal that detects a stalled relay.

[`docs/DESIGN_REVIEW.md` §16](docs/DESIGN_REVIEW.md) covers what production would require —
replica counts and pod topology, synchronous PostgreSQL replication for RPO 0, Kafka RF 3 with
`min.insync.replicas=2`, partition over-provisioning, Streams standby replicas — along with a
failure-by-failure walkthrough of observed behaviour and recovery, and a frank account of what
HA does *not* give you.

## Observability

Metrics are exposed in Prometheus format at **`/actuator/prometheus`** (and as JSON at
`/actuator/metrics`). Nothing scrapes them here — they are exposed so they can be read
directly:

```bash
curl -s localhost:8080/actuator/prometheus | grep '^payment_'
curl -s localhost:8081/actuator/prometheus | grep '^notification_'
```

Alongside the Spring Boot defaults (HTTP server timings, HikariCP pool state, JVM, Kafka
client), the application registers:

| Metric | Type | Purpose |
|---|---|---|
| `payment_attempts_total{outcome}` | counter | Attempts by terminal outcome |
| `payment_amount{currency}` | summary | Value of completed payments |
| `payment_execution_seconds` | timer | Transactional debit, lock acquisition to commit |
| `payment_lock_wait_seconds` | timer | Time waiting for the per-account row lock |
| `payment_outbox_pending` | gauge | Rows awaiting publication |
| `payment_outbox_oldest_pending_age_seconds` | gauge | Age of the oldest unpublished row |
| `payment_outbox_relay_last_poll_age_seconds` | gauge | Time since the relay last completed a poll |
| `payment_outbox_published_total` | counter | Rows published successfully |
| `payment_outbox_publish_failures_total` | counter | Failed publication attempts |
| `payment_outbox_parked_total` | counter | Rows parked after exhausting retries |
| `notification_events_total{outcome}` | counter | Payment events by what deduplication did: `notified`, `duplicate_suppressed`, `dead_lettered` |
| `notification_deliveries_total{outcome}` | counter | Delivery attempts: `sent`, `failed`, `dead_lettered` |
| `notification_deduplication_purged_total` | counter | Payment ids purged from the store after their retention |

`duplicate_suppressed` is the direct, observable evidence that deduplication is doing its job.

Three choices are worth calling out, since they are the ones that make the metrics usable
rather than merely present:

- **Outbox age is the primary SLI.** A stalled relay is invisible from the API — payments keep
  returning `201` while notifications silently stop. Age beats depth, because a deep but
  draining backlog is healthy and a shallow static one is not. The gauge holds the oldest row's
  *timestamp* and computes the age at scrape time, so it keeps growing even if the relay stops
  updating it; `relay_last_poll_age` covers a relay that died while the backlog was empty.
- **Every outcome counter is registered at zero on startup.** A counter that has never been
  incremented is absent from a Prometheus scrape entirely, so an alert filtering on a rare
  outcome would have no series to evaluate until the first occurrence — exactly when it needs
  to already work. A unit test enforces this.
- **Lock wait is timed separately from execution.** Both rising means a slow database; only the
  wait rising means contention on a hot account. That distinction is the whole diagnostic value.

Metric definitions live in one class
([`PaymentMetrics`](payment-service/src/main/java/com/alpian/payment/observability/PaymentMetrics.java))
rather than being scattered, because metric names are a published interface: dashboards and
alert rules reference them, so a rename breaks whoever is on call.

**Production improvement — shipping metrics onward.** The obvious next step is scraping this
endpoint into a hosted backend such as Datadog. The preferred route is the **Datadog Agent's
OpenMetrics check** (annotate the pod, the agent scrapes the existing endpoint) or an
**OpenTelemetry Collector** fanning out to one or more backends. Both keep the service
vendor-neutral, with no extra dependency and no API key in its configuration, so changing
vendor touches deployment config rather than application code — which is why they are preferred
over pushing directly with `micrometer-registry-datadog`. Distributed tracing
(`micrometer-tracing-bridge-otel`, with trace context propagated through Kafka headers) is the
other addition that would most improve debuggability across the two services.

[`docs/DESIGN_REVIEW.md` §17](docs/DESIGN_REVIEW.md) covers the full catalogue, the
instrumentation rationale, and suggested alert conditions.

## Scope exclusions

These are deliberate, to keep the exercise focused:

- **No authentication or authorization anywhere.** `userId` appears in the request path; in
  production it must come from a validated JWT subject, and the code says so at the relevant
  point. The service does still verify that the addressed account belongs to the addressed
  user — without that check the path would be a trivial IDOR, and it is business logic rather
  than authentication.
- **No Kafka security.** PLAINTEXT listeners, no SASL, no TLS, no ACLs.
- **No transport security**, no rate limiting, no quota enforcement.
- **No metric scraping, dashboards, alert rules or distributed tracing.** Metrics are exposed
  at `/actuator/prometheus` but nothing collects them; see the Observability section.
- **No Schema Registry.** Protobuf serdes are hand-rolled; a Schema Registry would own
  compatibility enforcement in production.
- **No account onboarding API.** Users and accounts are seeded by migration.
- **No FX.** A payment's currency must equal its account's currency.
- **Single-broker, single-replica Kafka** and a single PostgreSQL instance locally. Real high
  availability needs RF ≥ 3 with `min.insync.replicas=2` and a replicated database; the
  application-side design (stateless service, DB-enforced invariants, `SKIP LOCKED` relay) is
  what makes running multiple replicas correct.
