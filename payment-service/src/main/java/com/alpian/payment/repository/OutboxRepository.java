package com.alpian.payment.repository;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** The transactional outbox: events are enqueued in the payment transaction, then relayed. */
@Repository
public class OutboxRepository {

  private static final int MAX_ERROR_LENGTH = 1000;

  public record Message(long id, UUID paymentId, String key, byte[] payload, int attempts) {}

  private final JdbcClient jdbc;

  public OutboxRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public void enqueue(UUID paymentId, String key, byte[] payload) {
    jdbc.sql("INSERT INTO outbox (payment_id, partition_key, payload) VALUES (:id, :key, :payload)")
        .param("id", paymentId)
        .param("key", key)
        .param("payload", payload)
        .update();
  }

  /**
   * Locks up to {@code limit} due messages, oldest first, for the rest of the transaction. {@code
   * SKIP LOCKED} lets relays on several replicas take disjoint batches without coordination.
   */
  public List<Message> claimBatch(int limit) {
    return jdbc.sql(
            """
            SELECT id, payment_id, partition_key, payload, attempts
              FROM outbox
             WHERE published_at IS NULL AND parked_at IS NULL AND next_attempt_at <= now()
             ORDER BY id
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """)
        .param("limit", limit)
        .query(
            (rs, row) ->
                new Message(
                    rs.getLong("id"),
                    rs.getObject("payment_id", UUID.class),
                    rs.getString("partition_key"),
                    rs.getBytes("payload"),
                    rs.getInt("attempts")))
        .list();
  }

  public void markPublished(List<Long> ids) {
    if (!ids.isEmpty()) {
      jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
          .param("ids", ids)
          .update();
    }
  }

  public void retryLater(long id, String error, Duration delay) {
    jdbc.sql(
            """
            UPDATE outbox
               SET attempts = attempts + 1, last_error = :error,
                   next_attempt_at = now() + make_interval(secs => :delay)
             WHERE id = :id
            """)
        .param("id", id)
        .param("error", truncate(error))
        .param("delay", delay.toMillis() / 1000.0)
        .update();
  }

  /** Stops retrying a message; it stays in the table for inspection. */
  public void park(long id, String error) {
    jdbc.sql(
            """
            UPDATE outbox SET attempts = attempts + 1, last_error = :error, parked_at = now()
             WHERE id = :id
            """)
        .param("id", id)
        .param("error", truncate(error))
        .update();
  }

  public long countPending() {
    return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL AND parked_at IS NULL")
        .query(Long.class)
        .single();
  }

  private static String truncate(String error) {
    return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
  }
}
