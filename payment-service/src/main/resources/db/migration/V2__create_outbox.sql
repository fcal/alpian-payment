-- Transactional outbox.
--
-- Rows are inserted in the SAME database transaction as the balance debit, which makes the
-- event and the state change atomic. This removes the dual-write problem: there is no window
-- in which Kafka has an event that Postgres rolled back, nor a committed payment whose event
-- was lost. A relay polls this table and publishes to Kafka afterwards.
--
-- Delivery is therefore at-least-once (publish, then mark published -- a crash in between
-- republishes). Consumers must deduplicate; the notification service does so with a Kafka
-- Streams state store keyed by payment id.

CREATE TABLE outbox (
    id              BIGSERIAL   PRIMARY KEY,

    -- The payment this event describes. Also the consumer's deduplication key.
    aggregate_id    UUID        NOT NULL,

    -- The Kafka message key: the account id. Keying by account (not by payment) keeps an
    -- account's events on one partition and therefore ordered, and co-partitions them with
    -- the consumer's state store. A payment-id key would be unique per record, scattering an
    -- account's events across partitions and destroying per-account ordering.
    partition_key   TEXT        NOT NULL,

    event_type      TEXT        NOT NULL,
    -- Serialized protobuf. Opaque to Postgres; the schema lives in the `proto` module.
    payload         BYTEA       NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,

    -- Retry bookkeeping. `attempts` lets the relay park a row that repeatedly fails so one
    -- poison event cannot block the queue head; `next_attempt_at` implements backoff.
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,

    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

-- Partial index: the relay only ever scans unpublished rows, and this keeps the index small
-- regardless of how much history the table accumulates.
CREATE INDEX outbox_pending_idx
    ON outbox (next_attempt_at, id)
    WHERE published_at IS NULL;
