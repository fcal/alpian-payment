package com.alpian.payment.outbox;

import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.OutboxMessage;
import com.alpian.payment.repository.OutboxRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox rows to Kafka.
 *
 * <p>Each batch is claimed, published and marked in one database transaction. Holding the
 * transaction across the Kafka round trip is deliberate: the row locks are what keep a concurrent
 * relay on another replica from publishing the same rows, and they must last until the rows are
 * marked. The locks are on outbox rows only, never on accounts, so payments are unaffected however
 * long a publish takes; the producer timeouts bound that duration anyway.
 *
 * <p>Delivery is at-least-once. If the broker acknowledges a batch and the commit that marks it
 * then fails, those rows are published again on the next poll. Consumers deduplicate on the payment
 * id, so this is safe — and it is the trade the outbox makes: never lose an event, at the price of
 * occasionally sending one twice.
 *
 * <p>Not a Spring bean by itself: {@link OutboxConfiguration} declares it, and only when the relay
 * is enabled, so tests without a broker do not start one.
 */
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  /**
   * Bounds the work done in one poll while draining a backlog, so a large catch-up after an outage
   * proceeds in steps and the scheduler thread is regularly released.
   */
  private static final int MAX_BATCHES_PER_POLL = 10;

  static final String HEADER_PAYMENT_ID = "payment-id";
  static final String HEADER_EVENT_TYPE = "event-type";
  static final String HEADER_CONTENT_TYPE = "content-type";
  static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

  private final OutboxRepository outbox;
  private final KafkaTemplate<String, byte[]> kafka;
  private final TransactionTemplate transactions;
  private final PaymentMetrics metrics;
  private final OutboxProperties properties;
  private final Clock clock;

  public OutboxRelay(
      OutboxRepository outbox,
      KafkaTemplate<String, byte[]> kafka,
      TransactionTemplate transactions,
      PaymentMetrics metrics,
      OutboxProperties properties,
      Clock clock) {
    this.outbox = outbox;
    this.kafka = kafka;
    this.transactions = transactions;
    this.metrics = metrics;
    this.properties = properties;
    this.clock = clock;
  }

  /**
   * Scheduled entry point, registered by {@link OutboxConfiguration} with a fixed delay, so polls
   * on one instance never overlap: the next starts only after the previous has finished.
   */
  public void poll() {
    try {
      drain();
    } catch (RuntimeException e) {
      // Typically the database being unreachable. Logged and absorbed so the scheduler keeps
      // polling; recordRelayPoll() is not reached, so the last-poll gauge ages and alerts.
      log.error("Outbox relay poll failed", e);
    }
  }

  /**
   * Publishes due messages until none remain or the per-poll bound is reached.
   *
   * @return the number of messages claimed, whether or not they were published
   */
  public int drain() {
    int claimed = 0;
    for (int batch = 0; batch < MAX_BATCHES_PER_POLL; batch++) {
      int size = publishBatch();
      claimed += size;
      if (size < properties.batchSize()) {
        break;
      }
    }
    OutboxRepository.Backlog backlog = outbox.backlog();
    metrics.updateOutboxBacklog(backlog.pending(), backlog.oldestCreatedAt());
    metrics.recordRelayPoll();
    return claimed;
  }

  private int publishBatch() {
    Integer size =
        transactions.execute(
            status -> {
              List<OutboxMessage> batch =
                  outbox.claimBatch(properties.batchSize(), clock.instant());
              if (batch.isEmpty()) {
                return 0;
              }

              // Send the whole batch before waiting on any of it, so the producer can pipeline
              // and batch the requests instead of paying one round trip per message.
              List<CompletableFuture<SendResult<String, byte[]>>> sends = new ArrayList<>();
              for (OutboxMessage message : batch) {
                sends.add(send(message));
              }

              List<Long> published = new ArrayList<>();
              for (int i = 0; i < batch.size(); i++) {
                OutboxMessage message = batch.get(i);
                try {
                  sends.get(i).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                  published.add(message.id());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  failed(message, e);
                } catch (ExecutionException e) {
                  failed(message, e.getCause() == null ? e : e.getCause());
                } catch (java.util.concurrent.TimeoutException e) {
                  failed(message, new org.apache.kafka.common.errors.TimeoutException(e));
                }
              }

              outbox.markPublished(published, clock.instant());
              metrics.recordOutboxPublished(published.size());
              return batch.size();
            });
    return size == null ? 0 : size;
  }

  private CompletableFuture<SendResult<String, byte[]>> send(OutboxMessage message) {
    ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(properties.topic(), message.partitionKey(), message.payload());
    record.headers().add(HEADER_PAYMENT_ID, utf8(message.aggregateId().toString()));
    record.headers().add(HEADER_EVENT_TYPE, utf8(message.eventType()));
    record.headers().add(HEADER_CONTENT_TYPE, utf8(PROTOBUF_CONTENT_TYPE));
    try {
      return kafka.send(record);
    } catch (RuntimeException e) {
      // send() can fail synchronously -- notably when broker metadata cannot be fetched within
      // max.block.ms -- rather than through the future. Folded into the same path.
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Decides between retrying and parking.
   *
   * <p>The distinction is between failures time can fix and failures it cannot. Kafka marks the
   * former with {@link RetriableException}: an unreachable broker, a leader election, too few
   * in-sync replicas. Those are retried indefinitely with capped backoff and <em>never parked</em>,
   * however many attempts accumulate — otherwise a half-hour outage would park the entire backlog,
   * and a self-healing incident would need someone to replay every event by hand.
   *
   * <p>Everything else — a record too large, an unknown topic, an authorisation failure — will not
   * improve on its own. Those are retried a few times, in case the classification is wrong for a
   * transient case, and then parked for a human to look at, so one poison message cannot sit at the
   * head of the queue forever.
   */
  private void failed(OutboxMessage message, Throwable cause) {
    metrics.recordOutboxPublishFailure();
    int failures = message.attempts() + 1;
    Throwable root = rootKafkaCause(cause);
    String error = root.getClass().getSimpleName() + ": " + root.getMessage();
    boolean retriable = root instanceof RetriableException;

    if (!retriable && failures >= properties.maxAttempts()) {
      log.error(
          "Parking outbox message {} for payment {} after {} attempts: {}",
          message.id(),
          message.aggregateId(),
          failures,
          error);
      outbox.park(message.id(), error, clock.instant());
      metrics.recordOutboxParked();
      return;
    }

    Instant next = clock.instant().plus(properties.backoffAfter(message.attempts()));
    log.warn(
        "Failed to publish outbox message {} for payment {} (attempt {}, {}); retrying at {}: {}",
        message.id(),
        message.aggregateId(),
        failures,
        retriable ? "retriable" : "non-retriable",
        next,
        error);
    outbox.recordFailure(message.id(), error, next);
  }

  /**
   * The Kafka exception that actually describes the failure.
   *
   * <p>Spring's {@code KafkaTemplate} wraps producer failures in {@code KafkaProducerException},
   * which is not itself retriable, so testing the exception as received would classify every broker
   * outage as permanent and park the whole backlog after a few attempts. The cause chain is walked
   * to the first Kafka exception instead; failing that, the innermost cause.
   */
  static Throwable rootKafkaCause(Throwable thrown) {
    Throwable innermost = thrown;
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      // Kafka's own exception hierarchy. Spring's wrapper descends from Spring's separate
      // KafkaException, so it is passed over here and its cause examined instead.
      if (t instanceof org.apache.kafka.common.KafkaException) {
        return t;
      }
      innermost = t;
      if (t.getCause() == t) {
        break;
      }
    }
    return innermost;
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
