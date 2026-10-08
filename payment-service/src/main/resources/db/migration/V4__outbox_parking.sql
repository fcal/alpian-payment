-- Explicit parking for outbox rows the relay has given up on.
--
-- A separate migration rather than an edit to V2: applied migrations are immutable, and
-- changing V2 would fail Flyway's checksum validation on every database that already ran it.
--
-- A row is parked when publishing fails with a non-retriable error -- a record too large, a
-- serialisation fault -- that repetition will not fix. It then needs a human. Transient broker
-- failures are deliberately NOT parked, however long they last: an outage would otherwise park
-- the entire backlog and turn a self-healing incident into a manual one.
ALTER TABLE outbox ADD COLUMN parked_at TIMESTAMPTZ;

-- The relay scans only rows that are neither published nor parked; keep the partial index
-- matched to that predicate so parked rows do not accumulate in it.
DROP INDEX outbox_pending_idx;
CREATE INDEX outbox_pending_idx
    ON outbox (next_attempt_at, id)
    WHERE published_at IS NULL AND parked_at IS NULL;
