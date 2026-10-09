package com.alpian.payment.outbox;

import com.alpian.payment.repository.OutboxRepository;
import com.alpian.payment.repository.OutboxRepository.Message;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox messages to Kafka, at least once.
 *
 * <p>Each batch is claimed, sent and marked published in one transaction, so its row locks keep
 * other relays off it until it is marked. A crash between the broker ack and the commit republishes
 * the batch; consumers deduplicate on the payment id.
 */
@Component
@ConditionalOnProperty(name = "payment.outbox.relay-enabled", matchIfMissing = true)
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  public static final String HEADER_PAYMENT_ID = "payment-id";
  public static final String HEADER_EVENT_TYPE = "event-type";

  private final OutboxRepository outbox;
  private final KafkaTemplate<String, byte[]> kafka;
  private final TransactionTemplate transactions;
  private final OutboxProperties properties;

  public OutboxRelay(
      OutboxRepository outbox,
      KafkaTemplate<String, byte[]> kafka,
      TransactionTemplate transactions,
      OutboxProperties properties,
      MeterRegistry metrics) {
    this.outbox = outbox;
    this.kafka = kafka;
    this.transactions = transactions;
    this.properties = properties;
    // A growing backlog is the only symptom of a stalled relay: payments keep succeeding.
    Gauge.builder("payment.outbox.pending", outbox, OutboxRepository::countPending)
        .register(metrics);
  }

  @Scheduled(fixedDelayString = "${payment.outbox.poll-interval-ms:500}")
  public void poll() {
    try {
      while (publishBatch() == properties.batchSize()) {
        // keep draining while batches are full
      }
    } catch (RuntimeException e) {
      log.error("Outbox relay poll failed", e);
    }
  }

  /** Publishes one batch and returns how many messages were claimed. */
  public int publishBatch() {
    Integer claimed =
        transactions.execute(
            status -> {
              List<Message> batch = outbox.claimBatch(properties.batchSize());
              // Send everything before waiting, so the producer can pipeline the batch.
              List<CompletableFuture<SendResult<String, byte[]>>> sends =
                  batch.stream().map(this::send).toList();
              List<Long> published = new ArrayList<>();
              for (int i = 0; i < batch.size(); i++) {
                try {
                  sends.get(i).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                  published.add(batch.get(i).id());
                } catch (ExecutionException e) {
                  failed(batch.get(i), e.getCause());
                } catch (java.util.concurrent.TimeoutException e) {
                  failed(batch.get(i), new TimeoutException(e));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  failed(batch.get(i), e);
                }
              }
              outbox.markPublished(published);
              return batch.size();
            });
    return claimed == null ? 0 : claimed;
  }

  private CompletableFuture<SendResult<String, byte[]>> send(Message message) {
    ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(properties.topic(), message.key(), message.payload());
    record.headers().add(HEADER_PAYMENT_ID, utf8(message.paymentId().toString()));
    record.headers().add(HEADER_EVENT_TYPE, utf8(PaymentEvents.EVENT_TYPE));
    try {
      return kafka.send(record);
    } catch (RuntimeException e) {
      // send() fails synchronously when broker metadata is unavailable within max.block.ms.
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Retriable errors (broker down, leader election) are retried with backoff and never parked, so
   * an outage drains by itself. Other errors (record too large, ...) cannot heal: after a few
   * attempts the message is parked so it does not block the queue.
   */
  private void failed(Message message, Throwable error) {
    Throwable cause = kafkaCause(error);
    String description = cause.getClass().getSimpleName() + ": " + cause.getMessage();
    if (!(cause instanceof RetriableException)
        && message.attempts() + 1 >= properties.maxAttempts()) {
      log.error("Parking outbox message {}: {}", message.id(), description);
      outbox.park(message.id(), description);
    } else {
      log.warn("Failed to publish outbox message {}, will retry: {}", message.id(), description);
      outbox.retryLater(message.id(), description, properties.backoff(message.attempts()));
    }
  }

  /** Spring wraps producer errors in its own exception type; find Kafka's own cause. */
  private static Throwable kafkaCause(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof KafkaException) {
        return t;
      }
    }
    return error;
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
