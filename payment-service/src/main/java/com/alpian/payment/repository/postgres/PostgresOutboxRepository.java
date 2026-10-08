package com.alpian.payment.repository.postgres;

import com.alpian.payment.repository.OutboxMessage;
import com.alpian.payment.repository.OutboxRepository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** PostgreSQL {@link OutboxRepository}. */
@Repository
public class PostgresOutboxRepository implements OutboxRepository {

  /** Long enough to diagnose from, short enough that a pathological message cannot bloat rows. */
  private static final int MAX_ERROR_LENGTH = 1000;

  private static final String ENQUEUE =
      """
      INSERT INTO outbox (aggregate_id, partition_key, event_type, payload,
                          created_at, next_attempt_at)
      VALUES (:aggregateId, :partitionKey, :eventType, :payload, :createdAt, :createdAt)
      """;

  /**
   * {@code SKIP LOCKED} is what lets several relays run at once. A relay that finds a row locked by
   * another skips it instead of queueing behind it, so replicas take disjoint batches with no
   * leader election and no coordination. A replica that dies mid-batch releases its locks when its
   * connection closes, and the rows become claimable again on the next poll.
   *
   * <p>Ordered by {@code id}, so within one relay events go out in the order they were committed.
   * Across concurrent relays that order is not preserved — see §16.5 of the design review.
   */
  private static final String CLAIM_BATCH =
      """
      SELECT id, aggregate_id, partition_key, event_type, payload, created_at, attempts
        FROM outbox
       WHERE published_at IS NULL
         AND parked_at IS NULL
         AND next_attempt_at <= :now
       ORDER BY id
       LIMIT :limit
         FOR UPDATE SKIP LOCKED
      """;

  private static final String MARK_PUBLISHED =
      "UPDATE outbox SET published_at = :publishedAt WHERE id IN (:ids)";

  private static final String RECORD_FAILURE =
      """
      UPDATE outbox
         SET attempts = attempts + 1,
             last_error = :error,
             next_attempt_at = :nextAttemptAt
       WHERE id = :id
      """;

  private static final String PARK =
      """
      UPDATE outbox
         SET attempts = attempts + 1,
             last_error = :error,
             parked_at = :parkedAt
       WHERE id = :id
      """;

  private static final String BACKLOG =
      """
      SELECT count(*) AS pending, min(created_at) AS oldest
        FROM outbox
       WHERE published_at IS NULL
         AND parked_at IS NULL
      """;

  private final JdbcClient jdbc;

  public PostgresOutboxRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void enqueue(OutboxMessage message) {
    // Outside a transaction this would commit independently of the payment, reintroducing the
    // dual write the outbox exists to remove: an event for a payment that then rolled back.
    requireTransaction("enqueue() must join the payment transaction");
    jdbc.sql(ENQUEUE)
        .param("aggregateId", message.aggregateId())
        .param("partitionKey", message.partitionKey())
        .param("eventType", message.eventType())
        .param("payload", message.payload())
        .param("createdAt", Timestamp.from(message.createdAt()))
        .update();
  }

  @Override
  public List<OutboxMessage> claimBatch(int limit, Instant now) {
    // As with the account debit: in autocommit FOR UPDATE releases its locks the moment the select
    // returns, and two relays would happily publish the same rows.
    requireTransaction("claimBatch() must run inside a transaction to hold its row locks");
    return jdbc.sql(CLAIM_BATCH)
        .param("now", Timestamp.from(now))
        .param("limit", limit)
        .query(PostgresOutboxRepository::mapMessage)
        .list();
  }

  @Override
  public void markPublished(List<Long> ids, Instant publishedAt) {
    if (ids.isEmpty()) {
      return;
    }
    jdbc.sql(MARK_PUBLISHED)
        .param("publishedAt", Timestamp.from(publishedAt))
        .param("ids", ids)
        .update();
  }

  @Override
  public void recordFailure(long id, String error, Instant nextAttemptAt) {
    jdbc.sql(RECORD_FAILURE)
        .param("id", id)
        .param("error", truncate(error))
        .param("nextAttemptAt", Timestamp.from(nextAttemptAt))
        .update();
  }

  @Override
  public void park(long id, String error, Instant parkedAt) {
    jdbc.sql(PARK)
        .param("id", id)
        .param("error", truncate(error))
        .param("parkedAt", Timestamp.from(parkedAt))
        .update();
  }

  @Override
  public Backlog backlog() {
    return jdbc.sql(BACKLOG)
        .query(
            (rs, rowNum) -> {
              Timestamp oldest = rs.getTimestamp("oldest");
              return new Backlog(
                  rs.getLong("pending"), Optional.ofNullable(oldest).map(Timestamp::toInstant));
            })
        .single();
  }

  private static void requireTransaction(String message) {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(message);
    }
  }

  private static String truncate(String error) {
    if (error == null) {
      return null;
    }
    return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
  }

  private static OutboxMessage mapMessage(ResultSet rs, int rowNum) throws SQLException {
    return new OutboxMessage(
        rs.getLong("id"),
        rs.getObject("aggregate_id", UUID.class),
        rs.getString("partition_key"),
        rs.getString("event_type"),
        rs.getBytes("payload"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getInt("attempts"));
  }
}
