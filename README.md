# Payment Service

A REST service to check account balances and submit outbound payments. Transactions are recorded
in PostgreSQL, and the sender is notified asynchronously over Kafka. The focus is **high
availability, transactional processing and error handling**. Every decision, with the
alternatives rejected along the way, is argued in [`docs/DESIGN_REVIEW.md`](docs/DESIGN_REVIEW.md).

| Module | Responsibility |
|---|---|
| `proto` | Protobuf event contract shared by both services |
| `payment-service` | REST API, balance check and debit, payment journal, transactional outbox relay |
| `notification-service` | Kafka Streams deduplication of payment events, delivery of notifications |

```
POST /payments ─▶ [ lock account → check → debit → journal → outbox row ]   (one DB transaction)
                                                                │
          outbox relay ─▶ payment-events ─▶ Streams dedup ─▶ notification-events ─▶ delivery listener
                                                 │                                        │
                                                 └─▶ payment-events-dlt                   └─▶ notification-events-dlt
```

## Running locally

Requires Docker and JDK 21 (Gradle provisions the toolchain if needed).

```bash
./gradlew :payment-service:bootRun        # API on :8080
./gradlew :notification-service:bootRun   # actuator on :8081
```

`spring-boot-docker-compose` starts [`compose.yaml`](compose.yaml) (PostgreSQL and Kafka in KRaft
mode) and wires the connections, so no connection details are duplicated in configuration. Flyway
creates the schema and seeds demo data. To run the stack by hand instead:
`docker compose up -d`, then `SPRING_DOCKER_COMPOSE_ENABLED=false ./gradlew :payment-service:bootRun`.

Against the seeded demo account:

```bash
BASE=localhost:8080/api/v1/users/11111111-1111-1111-1111-111111111111/accounts/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1

curl -s $BASE/balance

curl -si -X POST $BASE/payments \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":{"value":"250.50","currency":"CHF"},
       "beneficiary":{"name":"Acme GmbH","iban":"CH93 0076 2011 6238 5295 7"},
       "reference":"Invoice 42"}'

# paymentId from the response above, or the last segment of its Location header
curl -s $BASE/payments/$PAYMENT_ID
```

Resending with the same key returns the original response with `Idempotent-Replayed: true` and
does not debit again. No notification is sent for a replayed request.

The notification service logs a `Notifying user ...` line for each payment.
`scripts/tail-events.sh [-f] [-t TOPIC]` decodes the records on a topic (needs
`brew install kcat protobuf`).

### Docker images

Each service has a `Dockerfile` packaging its boot jar on a JRE, running as a non-root user:

```bash
./gradlew :payment-service:bootJar :notification-service:bootJar
docker build -t payment-service payment-service
docker build -t notification-service notification-service
```

Both are configured through the environment: `POSTGRES_URL`, `POSTGRES_USER` and
`POSTGRES_PASSWORD` for the payment service, `SPRING_KAFKA_BOOTSTRAP_SERVERS` for both.

## Testing

```bash
./gradlew build          # format check, unit/integration tests, component tests
./gradlew test           # unit and integration tests (Testcontainers)
./gradlew componentTest  # end-to-end, both services as Docker images
./gradlew spotlessApply  # google-java-format
```

- **Payment service**: business rules against in-memory repositories; PostgreSQL concurrency
  (many threads on one account never overdraw, concurrent retries with one key debit once, lock
  timeout, the DB constraint); the HTTP contract; the outbox relay against real Kafka (outage
  retried rather than parked, poison message parked, two relays never duplicate).
- **Notification service**: the topology with `TopologyTestDriver` (deduplication, store purge,
  undecodable, invalid and wrongly keyed events dead-lettered), and against a real broker:
  duplicates delivered once, a bad event not blocking the partition, failed deliveries retried
  then dead-lettered, deduplication surviving the loss of local state.
- **Component tests** (`payment-service/src/component`): both services as the Docker images their
  `Dockerfile`s build, with PostgreSQL, Kafka and the topics from the same
  `docker/kafka/create-topics.sh` as the compose stack. The source set cannot see the services'
  classes; it talks HTTP, Kafka and SQL only, and follows each payment to the delivery log:

| Scenario | What it proves |
|---|---|
| Happy path | `201` → journal and balance → outbox published → event → notification → delivery |
| Retried request | Same `Idempotency-Key` twice: one debit, one event, one notification |
| Declined payment | `409 insufficient_funds`, balance unchanged, the payer told why |
| 20 concurrent payments on CHF 1000 | Exactly 10 succeed, the balance lands on 0.00, every attempt is notified |
| Kafka paused mid-test | The payment still returns `201` in well under 2s; the notification arrives after recovery |

## REST API

Swagger UI at **`/swagger-ui.html`**, OpenAPI at **`/v3/api-docs`**.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/users/{userId}/accounts/{accountId}/balance` | Current balance |
| `POST` | `/api/v1/users/{userId}/accounts/{accountId}/payments` | Submit a payment (`Idempotency-Key` header required) |
| `GET` | `/api/v1/users/{userId}/accounts/{accountId}/payments/{paymentId}` | Retrieve a payment, completed or declined |

| Status | When | `code` |
|---|---|---|
| `201` | Payment completed | — |
| `400` | Malformed body, invalid field, missing or over-long `Idempotency-Key` | `validation_failed` |
| `404` | No such account for this user (or not theirs) | `account_not_found` |
| `409` | Insufficient funds; the attempt is recorded and identified by `paymentId` / `Location` | `insufficient_funds` |
| `422` | Currency differs from the account's, or the key was used for a different payment | `currency_mismatch`, `idempotency_key_reused` |
| `503` | Another payment on the account is in progress; retry after `Retry-After` with the same key | `lock_timeout` |
| `503` | Database temporarily unreachable (e.g. primary failover); retry after `Retry-After` with the same key | `service_unavailable` |
| `500` | Anything unexpected; details are logged, not returned | `internal_error` |

Errors are RFC 9457 problem details; clients branch on the stable `code`, not the `detail` prose.
A replay returns the original status, so a client retrying after a timeout gets exactly the
response it missed, including a `409` for a declined payment. Amounts are decimal strings at the
currency's precision (`"250.50"`); requests accept a string or a number, and more than four
decimal places is refused rather than rounded.

## Design choices

**No double spending, enforced in three places.** The account row is locked
(`SELECT ... FOR UPDATE`) before the balance check, so concurrent payments on one account are
serialized by the database across all replicas, while different accounts proceed in parallel. A
client-supplied `Idempotency-Key`, unique per account, makes a retried request replay its
original outcome; a server-generated id cannot, since a retry would mint a new one. And
`CHECK (balance >= 0)` means no code path can overdraft, whatever the application does.

**Bounded contention.** A lock wait is capped by `payment.lock-timeout` (3s) and answered with a
retryable `503`, so a hot account cannot pin connections until the pool is exhausted.

**The event is atomic with the debit (transactional outbox).** The outbox row commits in the same
transaction as the debit and the journal entry, so Kafka never sees an event for a rolled-back
payment nor misses one for a committed payment. The payment path never talks to Kafka: **payments
keep succeeding while Kafka is down**, and the relay catches up afterwards. Every replica runs a
relay, claiming batches with `FOR UPDATE SKIP LOCKED`, so relays take disjoint rows without
coordinating. Retriable errors (unreachable broker) are retried indefinitely with backoff;
non-retriable ones (record too large) are parked after `max-attempts` so one poison message
cannot block the queue.

**Effectively-once notification.** Publishing is at least once, so the notification service
deduplicates: a Kafka Streams processor keeps notified payment ids in a changelog-backed state
store, and `exactly_once_v2` commits the store write, the output record and the input offset
together. Events are keyed by account id, which keeps an account's events ordered and every copy
of a payment's event on the same partition, hence the same store; a wrongly keyed event is
dead-lettered rather than checked against the wrong store. The store is purged after 8 days,
longer than the input topic's 7-day retention.

**Delivery outside the topology.** A plain `read_committed` listener sends the notification, with
retries then a dead-letter topic. It runs outside Streams because Streams re-runs an aborted
transaction and would repeat any side effect in it. The final hop to a person is at-least-once in
any system, so a real provider would take the payment id as its idempotency key. Here the sender
is a log line.

**Bad events never stop the stream.** An undecodable, invalid (missing amount, unknown currency
or status) or wrongly keyed event goes to `payment-events-dlt` with its original bytes and
`dlt-reason` / `dlt-error` headers. Anything that still escapes the topology shuts the Streams
client down, and a Streams health indicator in both liveness and readiness gets the instance
replaced instead of leaving it up but idle.

**Hand-written SQL with `JdbcClient`, not an ORM.** The task centres on locking primitives
(`FOR UPDATE`, conditional updates, `SKIP LOCKED`) that read clearly as SQL and are obscured by
JPA, whose read-mutate-flush idiom is the very race being avoided.

**Built for replicas.** The services are stateless, all coordination lives in PostgreSQL, and
invariants are enforced by the database, so they hold at any replica count and during a rolling
deploy running two versions. No transaction spans accounts, so the account id is a natural shard
key. Kafka is deliberately not a readiness dependency of the payment service, since the outbox
makes it non-critical; PostgreSQL is. Both services shut down gracefully, so a rolling deploy
lets in-flight requests finish.

**Data model.** `app_user 1:N account 1:N payment`. `payment` is an append-only journal of
outbound payments to external beneficiaries. Money is `NUMERIC(19,4)` / `BigDecimal`, never
floating point; timestamps are `TIMESTAMPTZ`.

## Observability

Prometheus metrics at `/actuator/prometheus`, health probes at
`/actuator/health/{liveness,readiness}`. Alongside the Spring Boot defaults (HTTP, HikariCP, JVM,
Kafka clients):

| Metric | Purpose |
|---|---|
| `payment_attempts_total{outcome}` | Attempts by terminal outcome, every outcome registered at zero so alerts have a series from startup |
| `payment_execution_seconds`, `payment_lock_wait_seconds` | Debit duration and lock wait, separated: only the wait rising means a hot account |
| `payment_database_unavailable_total` | Requests answered `503` because the database was unreachable |
| `payment_outbox_pending`, `payment_outbox_oldest_pending_age_seconds` | Outbox backlog. Age is the primary SLI: a stalled relay is invisible from the API |
| `payment_outbox_relay_last_poll_age_seconds` | Detects a relay that died while the backlog was empty |
| `payment_outbox_published_total`, `_publish_failures_total`, `_parked_total` | Relay throughput and failures |
| `notification_events_total{outcome}` | `notified`, `duplicate_suppressed`, `dead_lettered` |
| `notification_deliveries_total{outcome}` | `sent`, `failed`, `dead_lettered` |
| `notification_deduplication_purged_total` | Payment ids purged from the store after their retention |

The `paymentId` is in the MDC, so a payment can be traced in the logs from the API through the
outbox to the published event. [Design review §17](docs/DESIGN_REVIEW.md#17-observability) has
the full catalogue and suggested alerts.

## Scope exclusions and limitations

Deliberate, to keep the exercise focused:

- **No authentication or authorization.** `userId` in the path stands in for a validated JWT
  subject. The ownership check behind it is real: another user's account answers the same `404`
  as a missing one.
- **No transport or Kafka security** (PLAINTEXT, no SASL, TLS or ACLs), no rate limiting.
- **No Schema Registry.** Protobuf serdes are hand-rolled; compatibility is kept by convention
  (additive fields, unknown statuses dead-lettered rather than guessed).
- **No account onboarding API** (users and accounts are seeded), **no FX** (the payment currency
  must equal the account's), and **no internal account-to-account transfers**, which need
  two-sided journalling and deterministic lock ordering and are a different feature.
- **Notification is a log line**; there is no real email or push provider.
- **Development topology**: one instance of each service, one PostgreSQL, a single Kafka broker
  with replication factor 1.
- **No CI pipeline.** Stage 7 of the plan (GitHub Actions and a final review pass) was not
  implemented.

Limitations of the design itself:

- **Per-account throughput is bounded by the row lock.** Payments on one account are serialized,
  so a very hot account is limited to one debit per transaction latency, and excess load is shed
  as `503 lock_timeout`.
- **The relay polls** (every 500 ms), adding latency and steady load on the database. Change data
  capture (Debezium) would remove both, at the cost of more infrastructure.
- **Parked outbox rows and dead-lettered records** are kept with their reason, but there is no
  tooling to inspect or replay them.
- **Flyway runs on application startup**, which is convenient locally but means a failed migration
  takes down every instance that restarts.

## What production would require

[Design review §16](docs/DESIGN_REVIEW.md#16-high-availability-in-a-production-deployment) covers
this in detail, with a failure-by-failure walkthrough. In short:

- **Security**: authentication at the edge with `userId` taken from the token, TLS everywhere,
  Kafka SASL/TLS and ACLs, secrets from a secret store, rate limiting per client.
- **PostgreSQL**: a managed or Patroni-style primary with synchronous replication (RPO 0) and
  automatic failover; the API already answers `503` during the switch, and idempotent retries
  resolve to the original outcome. Migrations run as a separate step before rollout.
- **Kafka**: at least three brokers across zones, replication factor 3 with
  `min.insync.replicas=2`, over-provisioned partitions, a Schema Registry enforcing compatibility.
- **Services**: at least three replicas of each, spread across zones, with a
  `PodDisruptionBudget` and probes as configured here (readiness on PostgreSQL, liveness never on
  a dependency); Streams standby replicas for fast failover of the deduplication store.
- **Operations**: metrics scraped into a backend with dashboards and alerts on outbox age, relay
  liveness, lock timeouts and dead letters; distributed tracing propagated through Kafka headers;
  replay tooling for parked rows and dead-letter topics; a CI pipeline running the full build,
  including component tests.
- **Notifications**: a real provider, called with the payment id as its idempotency key.
