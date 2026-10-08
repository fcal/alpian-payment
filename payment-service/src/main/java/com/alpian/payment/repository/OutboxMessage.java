package com.alpian.payment.repository;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * An event awaiting publication, as stored in the outbox.
 *
 * @param id database sequence, null until enqueued. Also the publication order.
 * @param aggregateId the payment the event describes; the consumer's deduplication key
 * @param partitionKey the Kafka message key — the account id, so an account's events share a
 *     partition
 * @param payload serialised protobuf. Opaque here, so the outbox does not depend on the schema.
 * @param attempts publication attempts so far
 */
public record OutboxMessage(
    Long id,
    UUID aggregateId,
    String partitionKey,
    String eventType,
    byte[] payload,
    Instant createdAt,
    int attempts) {

  public OutboxMessage {
    Objects.requireNonNull(aggregateId, "aggregateId");
    Objects.requireNonNull(partitionKey, "partitionKey");
    Objects.requireNonNull(eventType, "eventType");
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(createdAt, "createdAt");
  }

  /** A message not yet stored. */
  public static OutboxMessage pending(
      UUID aggregateId, String partitionKey, String eventType, byte[] payload, Instant createdAt) {
    return new OutboxMessage(null, aggregateId, partitionKey, eventType, payload, createdAt, 0);
  }
}
