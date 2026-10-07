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
| 2 | PostgreSQL implementation, row locking, concurrency tests | ⏳ Next |
| 3 | REST API, error contract, OpenAPI | — |
| 4 | Outbox relay → Kafka | — |
| 5 | Notification service (Kafka Streams deduplication) | — |
| 6 | End-to-end component tests | — |
| 7 | Documentation and CI | — |

## Modules

| Module | Responsibility |
|---|---|
| `proto` | Protobuf definitions of the Kafka event contract, shared by both services |
| `payment-service` | REST API, payment execution, transaction journal, outbox relay |
| `notification-service` | Kafka Streams application: deduplicates payment events, notifies the sender |

## Running locally

Requires Docker and a JDK 21 toolchain (Gradle provisions the toolchain if absent).

```bash
./gradlew :payment-service:bootRun
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

Component tests live in a dedicated `src/component` source set in `payment-service`: they
drive the assembled service against real PostgreSQL and Kafka containers via Testcontainers,
so they are slower than unit tests and are kept separately runnable.

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
`exactly_once_v2` making the store update and the output record commit together. Note the
precise claim: this makes notification *effectively* once; the final side effect of delivering
a message to a person is at-least-once in any system.

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
| `payment_outbox_published_total` | counter | Rows published successfully |
| `payment_outbox_publish_failures_total` | counter | Failed publication attempts |
| `payment_outbox_parked_total` | counter | Rows parked after exhausting retries |

Three choices are worth calling out, since they are the ones that make the metrics usable
rather than merely present:

- **Outbox age is the primary SLI.** A stalled relay is invisible from the API — payments keep
  returning `201` while notifications silently stop. Age beats depth, because a deep but
  draining backlog is healthy and a shallow static one is not.
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
