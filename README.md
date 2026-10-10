# Payment Service

A REST service to check account balances and submit outbound payments. Transactions are recorded
in PostgreSQL, and the sender is notified asynchronously over Kafka. The focus is high
availability, transactional processing and error handling. Design decisions and trade-offs are in
[`docs/DESIGN_REVIEW.md`](docs/DESIGN_REVIEW.md).

| Module | Responsibility |
|---|---|
| `proto` | Protobuf event contract shared by both services |
| `payment-service` | REST API, balance check and debit, payment journal, transactional outbox relay |
| `notification-service` | Kafka Streams deduplication of payment events, delivery of notifications |

```
POST /payments ─▶ [ lock account → check → debit → journal → outbox ]  (one DB transaction)
                                                            │
                                    outbox relay ─▶ payment-events ─▶ Streams dedup ─▶ notification-events ─▶ delivery
```

## Running

Requires Docker and JDK 21 (Gradle provisions the toolchain if needed).

```bash
./gradlew :payment-service:bootRun        # API on :8080
./gradlew :notification-service:bootRun   # actuator on :8081
```

`spring-boot-docker-compose` starts [`compose.yaml`](compose.yaml) (PostgreSQL and Kafka) and
wires the connections. Flyway creates the schema and seeds demo accounts.

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
`scripts/tail-events.sh [-f] [-t TOPIC]` decodes the events on a topic (needs `kcat` and `protoc`).

## Testing

```bash
./gradlew build          # format check, unit/integration tests, component tests
./gradlew test           # unit and integration tests (Testcontainers)
./gradlew componentTest  # end-to-end, both services as Docker images
./gradlew spotlessApply  # google-java-format
```

- **Payment service**: business rules, concurrency (32 threads on one account never overdraw;
  32 concurrent retries with one key debit once; lock timeout; the DB constraint), the HTTP
  contract, and the outbox relay against real Kafka (outage retried rather than parked, poison
  message parked, two relays never duplicate).
- **Notification service**: topology with `TopologyTestDriver`, and against a real broker:
  duplicates delivered once, failed deliveries retried then dead-lettered, deduplication surviving
  the loss of local state.
- **Component tests** (`payment-service/src/component`): both services as Docker images with
  Postgres and Kafka. They cover the happy path to delivery, a retry notified once, a decline
  notified, 20 concurrent payments draining the account exactly, and a paused Kafka not
  affecting payments.

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
| `400` | Invalid request, missing or over-long `Idempotency-Key` | `validation_failed` |
| `404` | No such account for this user | `account_not_found` |
| `409` | Insufficient funds; the attempt is recorded and identified by `paymentId` / `Location` | `insufficient_funds` |
| `422` | Currency differs from the account's, or the key was used for a different payment | `currency_mismatch`, `idempotency_key_reused` |
| `503` | Another payment on the account is in progress; retry after `Retry-After` with the same key | `lock_timeout` |

Errors are RFC 9457 problem details; clients branch on `code`. Amounts are decimal strings. A
replay returns the original status, including `409` for a declined payment.

`userId` in the path stands in for authentication, which is out of scope. The ownership check is
real, though: another user's account answers `404`, the same as a missing one.

## Key guarantees

- **No double spending.** The account row is locked (`SELECT ... FOR UPDATE`) before the balance
  check, so concurrent payments on one account run one after another, across all replicas.
  Idempotency keys (unique per account) make retries safe. `CHECK (balance >= 0)` is the last line
  of defence.
- **Atomic events.** The outbox row commits with the debit, so Kafka never sees an event for a
  rolled-back payment and never misses one for a committed payment. **Payments keep succeeding
  while Kafka is down**; the relay catches up afterwards.
- **No double notifications.** The relay delivers at least once. The notification service keeps
  notified payment ids in a Kafka Streams store, with `exactly_once_v2` committing the store and
  the output together. Events are keyed by account id, so every copy of an event reaches the same
  store.
- **Bounded contention.** A lock wait is capped by `payment.lock-timeout` (3s) and answered with a
  retryable `503`, so a hot account cannot exhaust the connection pool.

## Outbox and notifications

```
payment-service                          notification-service
───────────────                          ────────────────────
POST /payments
  debit + payment + outbox row
  (one DB transaction)
         │
OutboxRelay (polls every 500 ms)
         │
         ▼
  [payment-events] ──▶ Kafka Streams dedup ("notified-payments" store)
                                │  seen payment id → drop
                                ▼
                      [notification-events]
                                │
                       NotificationListener ──▶ NotificationSender
                                │  retries exhausted
                                ▼
                      [notification-events-dlt]
```

- **Outbox.** The event is written as a row in the same transaction as the debit, and
  `OutboxRelay` publishes pending rows to Kafka. Publishing is at least once: a crash between the
  broker ack and marking the row published sends it again.
- **Deduplication.** A Kafka Streams processor drops any payment id already in its state store
  and emits one notification for each new one. The store write, the output and the input offset
  commit together (`exactly_once_v2`).
- **Delivery.** A plain `@KafkaListener` reads committed notifications and calls the sender,
  retrying with backoff before dead-lettering. It runs outside Streams so that a retried Streams
  transaction cannot send a notification twice. For the sake of the assignment, notification is just a log line.

## Observability

Prometheus metrics at `/actuator/prometheus`, health probes at `/actuator/health/{liveness,readiness}`.

| Metric | Meaning |
|---|---|
| `payment_attempts_total{outcome}` | Payment requests by outcome |
| `payment_outbox_pending` | Events awaiting publication; growth means a stalled relay |
| `notification_events_total{outcome}` | `notified`, `duplicate`, `skipped` (undecodable or invalid) |
| `notification_deliveries_dead_lettered_total` | Notifications that failed every retry |

## Out of scope

Authentication and transport security, Kafka security, Schema Registry, account onboarding (demo
accounts are seeded), FX (payment currency must match the account), internal transfers, and a
production topology (single broker, single database). See the
[design review](docs/DESIGN_REVIEW.md#8-high-availability-in-production) for what production needs.
