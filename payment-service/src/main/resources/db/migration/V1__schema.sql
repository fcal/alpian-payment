-- Users are not modelled: account.user_id identifies the owner. Payments are outbound only,
-- from an account to an external beneficiary.

CREATE TABLE account (
    id         UUID          PRIMARY KEY,
    user_id    UUID          NOT NULL,
    -- NUMERIC, never floating point, for money.
    balance    NUMERIC(19,4) NOT NULL,
    currency   CHAR(3)       NOT NULL,
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- The last line of defence against overdraft, whatever the application does.
    CONSTRAINT account_balance_non_negative CHECK (balance >= 0)
);

-- Append-only journal of payment attempts, declined ones included.
CREATE TABLE payment (
    id               UUID          PRIMARY KEY,
    account_id       UUID          NOT NULL REFERENCES account (id),
    idempotency_key  TEXT          NOT NULL,
    amount           NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    currency         CHAR(3)       NOT NULL,
    beneficiary_name TEXT          NOT NULL,
    beneficiary_iban TEXT          NOT NULL,
    reference        TEXT,
    status           TEXT          NOT NULL CHECK (status IN ('COMPLETED', 'FAILED')),
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ   NOT NULL
);

-- Idempotency keys are unique per account: a retried request can never create a second payment.
CREATE UNIQUE INDEX payment_account_idempotency_key_uidx ON payment (account_id, idempotency_key);

-- Transactional outbox: rows are written in the payment transaction and relayed to Kafka.
CREATE TABLE outbox (
    id              BIGSERIAL   PRIMARY KEY,
    payment_id      UUID        NOT NULL,
    -- Kafka key: the account id, so an account's events stay ordered on one partition.
    partition_key   TEXT        NOT NULL,
    payload         BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    -- Set when the relay gives up on a message it cannot publish.
    parked_at       TIMESTAMPTZ,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT
);

CREATE INDEX outbox_pending_idx ON outbox (next_attempt_at, id)
    WHERE published_at IS NULL AND parked_at IS NULL;
