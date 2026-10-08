package com.alpian.payment.repository.inmemory;

import com.alpian.payment.repository.OutboxMessage;
import com.alpian.payment.repository.OutboxRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link OutboxRepository} for unit tests: records what the payment path enqueues.
 *
 * <p>Only {@link #enqueue} is meaningful. The relay's operations depend on row locking and
 * transactions, which a list cannot model, so they are verified against PostgreSQL instead and fail
 * loudly here rather than pretending to work.
 */
public class InMemoryOutboxRepository implements OutboxRepository {

  private final List<OutboxMessage> messages = new CopyOnWriteArrayList<>();
  private final AtomicLong sequence = new AtomicLong();

  @Override
  public void enqueue(OutboxMessage message) {
    messages.add(
        new OutboxMessage(
            sequence.incrementAndGet(),
            message.aggregateId(),
            message.partitionKey(),
            message.eventType(),
            message.payload(),
            message.createdAt(),
            0));
  }

  /** Everything enqueued so far, in order. */
  public List<OutboxMessage> enqueued() {
    return new ArrayList<>(messages);
  }

  @Override
  public List<OutboxMessage> claimBatch(int limit, Instant now) {
    throw unsupported();
  }

  @Override
  public void markPublished(List<Long> ids, Instant publishedAt) {
    throw unsupported();
  }

  @Override
  public void recordFailure(long id, String error, Instant nextAttemptAt) {
    throw unsupported();
  }

  @Override
  public void park(long id, String error, Instant parkedAt) {
    throw unsupported();
  }

  @Override
  public Backlog backlog() {
    return new Backlog(
        messages.size(),
        messages.stream()
            .map(OutboxMessage::createdAt)
            .min(Instant::compareTo)
            .map(Optional::of)
            .orElse(Optional.empty()));
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException(
        "Relay operations need row locking; test them against PostgreSQL");
  }
}
