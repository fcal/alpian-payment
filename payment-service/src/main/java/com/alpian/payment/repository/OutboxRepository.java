package com.alpian.payment.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The transactional outbox.
 *
 * <p>{@link #enqueue} is called inside the payment transaction, so an event exists if and only if
 * the payment committed. Everything else serves the relay that publishes those events afterwards.
 */
public interface OutboxRepository {

  /** Stores an event. Must join the caller's transaction, which is the whole point. */
  void enqueue(OutboxMessage message);

  /**
   * Claims up to {@code limit} messages due for publication, oldest first, locking them for the
   * rest of the caller's transaction.
   *
   * <p>Rows already locked by another relay are skipped rather than waited for, so concurrent
   * relays — one per service replica — take disjoint batches without coordinating.
   *
   * @param now messages whose next attempt is after this are not yet due
   */
  List<OutboxMessage> claimBatch(int limit, Instant now);

  void markPublished(List<Long> ids, Instant publishedAt);

  /** Records a failed attempt and schedules the next one. */
  void recordFailure(long id, String error, Instant nextAttemptAt);

  /** Records a failed attempt and stops retrying the message. */
  void park(long id, String error, Instant parkedAt);

  /** Size and oldest entry of the unpublished, unparked backlog. */
  Backlog backlog();

  /**
   * @param oldestCreatedAt empty when nothing is pending
   */
  record Backlog(long pending, Optional<Instant> oldestCreatedAt) {}
}
