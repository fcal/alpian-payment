# Design Review

The main design decisions, the alternatives that were rejected, and what a production deployment
would add.

## 1. Payment flow: synchronous, one transaction

One database transaction does: lock the account → check ownership → replay check → currency
check → balance check → debit → insert `payment` → insert `outbox` row → commit. The API returns
the final outcome (`201` or `409`).

- **No `PENDING` state.** The transaction takes milliseconds and is atomic, so a payment is never
  observably in flight. A reservation flow against an external payment rail (insert `PENDING`,
  confirm later) would need one. That is the extension path, not built here.
- **Declines are journalled** as `FAILED` and notified too, so every request with a valid account
  has a recorded outcome, and replaying its key has something to return.
- **Outbound only.** Money leaves for an external beneficiary (name + IBAN), so a one-sided
  `payment` row is correct. Internal transfers would be a different feature: two-sided
  journalling, locking both accounts in a fixed order to avoid deadlocks, and FX.

## 2. Concurrency and double spending

| Threat | Guard |
|---|---|
| Two concurrent payments both pass the balance check | `SELECT ... FOR UPDATE` on the account row before any check |
| A client retries after a timeout | Client-supplied `Idempotency-Key`; the stored outcome is replayed |
| Two concurrent retries with the same key | They queue on the row lock: the second one sees the first one's committed row and replays it |
| Any bug that bypasses the above | `CHECK (balance >= 0)` and `UNIQUE (account_id, idempotency_key)` |

**Why not a check in the application?** "Is there an ongoing transaction?" in Java is check-then-act,
so two threads can both pass it. An in-JVM lock fixes that for one instance and breaks with two
replicas. The database lock holds across any number of replicas.

**Why not a server-generated id?** A retried POST would get a new id and pay a second time. The
server-generated `payment.id` is the correlation and deduplication id downstream. The
idempotency key is what makes retries safe.

**Pessimistic vs optimistic locking.** A conditional
`UPDATE ... WHERE balance >= :amount AND version = :v` holds no lock, but needs retry logic and
wastes work under contention. Pessimistic locking was chosen because it is simpler to reason
about. Its costs:

- Payments on one account run one at a time. Different accounts run in parallel.
- A waiting request holds a pool connection. `SET LOCAL lock_timeout` bounds the wait, and a
  timeout becomes `503` with `Retry-After`, so a burst on one hot account cannot exhaust the pool.
  `SET LOCAL` rather than a session setting, so that it still works behind PgBouncer in
  transaction mode.
- The lock is held until commit, so the transaction does no network I/O. Kafka is reached only
  through the outbox.

**A key identifies one attempt.** Replaying the key of a declined payment returns that decline,
even if the account has been funded since. Reusing a key for a different payment (another amount,
beneficiary or reference) is refused with `422` rather than replayed: a replay would tell the
client that a payment it never made had succeeded. Ownership is checked before the replay lookup,
so another user's key reveals nothing.

## 3. Persistence: `JdbcClient` and SQL, not JPA

The task is about locking primitives: `FOR UPDATE`, `SKIP LOCKED`, relative updates. These are
clear in SQL. JPA's usual flow of loading an entity, changing it and letting the flush write it is
exactly the read-modify-write race this design avoids. With three tables, an ORM adds little.

Money is `NUMERIC(19,4)` / `BigDecimal`, never floating point. Timestamps are `TIMESTAMPTZ`.

## 4. Events: transactional outbox and a polling relay

Publishing to Kafka directly from the payment path is a dual write: the event can go out while
the transaction rolls back, or the reverse. The outbox row is inserted **in the payment
transaction**, so an event exists if and only if the payment committed. As a result, **payments
keep working while Kafka is down**, and only notifications are delayed.

**Relay.** A `@Scheduled` poller on every replica claims batches with
`FOR UPDATE SKIP LOCKED`. Replicas take disjoint rows without any leader election. A relay that
dies mid-batch releases its locks when its connection closes. A batch is published and marked in
the same transaction, which gives at-least-once delivery: consumers deduplicate on the payment id.

**Retries.** Retriable Kafka errors (broker unreachable, leader election) are retried with
exponential backoff capped at `max-backoff` and are **never parked**. Otherwise an outage would
park the whole backlog and need a manual replay. Non-retriable errors (for example a record too
large) are parked after `max-attempts`, so one poison message cannot block the queue.

**Debezium** (CDC on the outbox table with the outbox event router) is the production alternative.
It removes the poller but needs Kafka Connect and logical replication. The poller needs no extra
infrastructure and keeps the pattern visible.

**Ordering caveat.** With several relays, two payments on one account can be published in either
order. Deduplication does not care. An order-sensitive consumer would need the relay sharded by
key.

## 5. Topics and keys

- **Key = account id.** An account's events stay ordered on one partition, and every copy of an
  event lands on the same partition, which per-partition deduplication relies on. A payment-id
  key would scatter an account's events across partitions.
- **`cleanup.policy=delete`, 7-day retention.** Events are facts, not state. Compacting by a key
  that is unique per record would keep every record forever.
- **Protobuf** with `Money` as a decimal string (protobuf has no decimal type) and
  `google.protobuf.Timestamp`. A Schema Registry would enforce compatibility in production.

## 6. Notifications: Kafka Streams deduplication

A plain consumer would need its own durable record of which payment ids were already notified. A
Kafka Streams state store keeps it in Kafka itself (backed by a changelog topic), and
`exactly_once_v2` commits the store write, the emitted notification and the input offset
together. A crash therefore cannot notify without also recording the id.

**Delivery is outside the topology.** Streams re-runs an aborted transaction, so an email sent
from inside a processor could be sent twice. A separate `read_committed` listener delivers each
notification, retries with backoff, and then moves it to `notification-events-dlt`. A crash
between sending and committing the offset still sends again, so a real provider should receive the
payment id as its idempotency key. Notification is *effectively* once. The last hop to a human is
at-least-once in any system.

**Simplifications.** An undecodable event is logged and skipped rather than dead-lettered. The
store is not purged, so it grows with the number of payments. Production would expire ids older
than the input topic's retention, with a punctuator or a windowed store.

## 7. Error handling

RFC 9457 problem details with a stable `code` property:

| Condition | Status | `code` |
|---|---|---|
| Malformed body, invalid field, missing or over-long key, unknown currency | 400 | `validation_failed` |
| Unknown account, or another user's account (same response) | 404 | `account_not_found` |
| Insufficient funds (recorded; `paymentId` + `Location` returned) | 409 | `insufficient_funds` |
| Currency mismatch / key reused for a different payment | 422 | `currency_mismatch` / `idempotency_key_reused` |
| Account lock not acquired in time | 503 + `Retry-After` | `lock_timeout` |
| Anything unexpected (details logged, not returned) | 500 | `internal_error` |

Postgres reports a lock timeout as SQLSTATE `55P03`, which Spring leaves uncategorised. The
repository translates it into `CannotAcquireLockException`, so that contention becomes a
retryable `503` instead of an opaque `500`.

## 8. High availability in production

The application is ready for multiple replicas: it is **stateless**, its **invariants live in the
database**, and **no transaction spans two accounts**, so the account id could become a shard key.
Idempotency also makes failover safe: when a primary dies mid-request, the client retries with the
same key.

What a production deployment would add:

| | Here | Production |
|---|---|---|
| Service replicas | 1 | ≥ 3 across zones, PodDisruptionBudget; readiness depends on Postgres, liveness does not |
| Postgres | 1 container | Primary + synchronous standby (`synchronous_commit=on`, RPO 0); multi-host JDBC URL with `targetServerType=primary`; PgBouncer, since pool size × replicas quickly exceeds `max_connections` |
| Kafka | 1 broker, RF 1, 3 partitions | ≥ 3 brokers across zones, RF 3, `min.insync.replicas=2`, `unclean.leader.election.enable=false`, more partitions from the start (they cannot be reduced, and adding them breaks key affinity) |
| Streams | — | `num.standby.replicas ≥ 1` for fast failover; changelog RF 3 |
| Migrations | Flyway on startup | A separate step before rollout, backward compatible with the running version |
| Security | none | Authentication (JWT subject instead of the `userId` path segment), TLS, Kafka mTLS/SASL with ACLs |

**Behaviour under failure:**

| Failure | Effect |
|---|---|
| Kafka down | Payments still return `201`; outbox rows accumulate and drain on recovery |
| Notification service down | Consumer lag grows; no payment impact; the store is restored from its changelog |
| Relay dies mid-batch | Locks are released and another relay reclaims the rows; at worst a duplicate, which is deduplicated |
| Postgres primary fails | `503`s for the failover window; clients retry with the same key |

**What to monitor:** outbox backlog (`payment_outbox_pending`): a stalled relay is invisible from
the API. Also consumer lag, the `lock_timeout` rate (a hot account), and replication health.

HA does not cover a region loss (cross-region replication is asynchronous, so RPO > 0), bad
deploys, or data corruption: those need backups with point-in-time recovery. The append-only
journal makes state reconstructable after such a failure.
