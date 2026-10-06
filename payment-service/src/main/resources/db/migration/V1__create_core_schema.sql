-- Core schema for the payment service.
--
-- Cardinality (deliberate choice, see docs/DESIGN_REVIEW.md):
--   one user  -> many accounts
--   one account -> exactly one user
--   one payment -> exactly one account (the debited account)
--
-- Payments are OUTBOUND ONLY: funds leave a held account toward an external beneficiary.
-- The counterparty is outside this system, so no credit side is recorded and `payment` is
-- deliberately one-sided. Internal account-to-account transfer is out of scope.

-- `app_user` rather than `user`: `user` is a reserved word in PostgreSQL and would need
-- double-quoting at every single usage.
CREATE TABLE app_user (
    id         UUID        PRIMARY KEY,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE account (
    id              UUID          PRIMARY KEY,
    user_id         UUID          NOT NULL REFERENCES app_user (id),
    -- NUMERIC(19,4), never a floating point type: binary floats cannot represent decimal
    -- fractions exactly and accumulate error across arithmetic. Maps to java.math.BigDecimal.
    balance         NUMERIC(19,4) NOT NULL,
    currency        CHAR(3)       NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- The final authority on overdraft. Application-level balance checks are a usability
    -- feature (clear error messages); this constraint is the guarantee. No code path,
    -- however buggy, can drive an account negative.
    CONSTRAINT account_balance_non_negative CHECK (balance >= 0),
    CONSTRAINT account_currency_is_iso4217 CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE INDEX account_user_id_idx ON account (user_id);

CREATE TABLE payment (
    id                 UUID          PRIMARY KEY,
    account_id         UUID          NOT NULL REFERENCES account (id),

    -- Client-supplied, via the `Idempotency-Key` request header. A server-generated id
    -- cannot deduplicate client retries: a retried POST would mint a new id and execute a
    -- second payment. The unique index below is what actually prevents double spending on
    -- retry; `id` is the correlation id carried through to Kafka.
    idempotency_key    TEXT          NOT NULL,

    amount             NUMERIC(19,4) NOT NULL,
    -- Must equal account.currency: no FX in scope. Enforced in the service layer, which can
    -- report a precise error; denormalised here so the journal row is self-contained even if
    -- the account's currency were ever migrated.
    currency           CHAR(3)       NOT NULL,

    beneficiary_name   TEXT          NOT NULL,
    beneficiary_iban   TEXT          NOT NULL,
    reference          TEXT,

    -- Terminal states only. A synchronous single-transaction debit is never observably
    -- in-flight, so no PENDING state exists: modelling one that cannot be read back would
    -- mislead. A reservation-based flow against an external rail would add it.
    status             TEXT          NOT NULL,
    failure_reason     TEXT,

    created_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT payment_amount_positive CHECK (amount > 0),
    CONSTRAINT payment_currency_is_iso4217 CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT payment_status_is_terminal CHECK (status IN ('COMPLETED', 'FAILED')),
    -- A failure must be explained; a success must not carry a failure reason.
    CONSTRAINT payment_failure_reason_matches_status CHECK (
        (status = 'FAILED' AND failure_reason IS NOT NULL)
        OR (status = 'COMPLETED' AND failure_reason IS NULL)
    )
);

-- Idempotency is scoped per account: two different accounts may legitimately reuse a key.
-- This index is the double-spend guard on retry -- a second insert with the same key fails,
-- and the service replays the stored outcome instead of debiting again.
CREATE UNIQUE INDEX payment_account_idempotency_key_uidx
    ON payment (account_id, idempotency_key);

-- Payment history, most recent first.
CREATE INDEX payment_account_created_at_idx ON payment (account_id, created_at DESC);

-- The payment table is an append-only journal: rows are never updated or deleted. Enforced
-- by convention and by the repository implementation rather than by a rule/trigger, to keep
-- the schema readable; a production system would add a REVOKE on UPDATE/DELETE.
