package com.alpian.notification.streams;

import com.alpian.notification.observability.NotificationMetrics;
import com.alpian.notification.observability.NotificationMetrics.EventOutcome;
import com.alpian.notification.streams.Notifications.Rejection;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits one notification per payment, however many times its event is delivered.
 *
 * <p>The outbox delivers at least once, so the same event can arrive more than once. The store
 * remembers each payment id already notified, with the time it was first seen; a later copy finds
 * its id there and is dropped. Under {@code exactly_once_v2} the store write, the emitted
 * notification and the input offset commit in one transaction, so a crash cannot separate "the
 * notification went out" from "the id was recorded".
 *
 * <p>The store is local to a task, i.e. to one input partition. That is correct only because every
 * copy of a payment's event carries the same key — its account id — and so lands on the same
 * partition. Records keyed otherwise are dead-lettered rather than risk a check against the wrong
 * partition's store.
 *
 * <p>The store is bounded by a wall-clock punctuator that deletes ids older than the retention.
 * Wall-clock rather than stream time, so that purging continues on a partition receiving no
 * traffic.
 */
final class DeduplicationProcessor implements Processor<String, byte[], String, byte[]> {

  private static final Logger log = LoggerFactory.getLogger(DeduplicationProcessor.class);

  static final String EVENT_TYPE_HEADER = "event-type";
  static final String NOTIFICATION_EVENT_TYPE = "alpian.payment.v1.NotificationEvent";

  private final String storeName;
  private final Duration retention;
  private final Duration purgeInterval;
  private final NotificationMetrics metrics;

  private ProcessorContext<String, byte[]> context;
  private KeyValueStore<String, Long> notified;

  DeduplicationProcessor(
      String storeName, Duration retention, Duration purgeInterval, NotificationMetrics metrics) {
    this.storeName = storeName;
    this.retention = retention;
    this.purgeInterval = purgeInterval;
    this.metrics = metrics;
  }

  @Override
  public void init(ProcessorContext<String, byte[]> context) {
    this.context = context;
    this.notified = context.getStateStore(storeName);
    context.schedule(purgeInterval, PunctuationType.WALL_CLOCK_TIME, this::purgeExpired);
  }

  @Override
  public void process(Record<String, byte[]> record) {
    PaymentEvent event;
    try {
      if (record.value() == null) {
        deadLetter(record, new Rejection(DeadLetterReason.UNDECODABLE, "record has no value"));
        return;
      }
      event = PaymentEvent.parseFrom(record.value());
    } catch (InvalidProtocolBufferException e) {
      deadLetter(record, new Rejection(DeadLetterReason.UNDECODABLE, e.getMessage()));
      return;
    }

    Optional<Rejection> rejection = Notifications.validate(event, record.key());
    if (rejection.isPresent()) {
      deadLetter(record, rejection.get());
      return;
    }

    String paymentId = event.getPaymentId();
    if (notified.get(paymentId) != null) {
      log.info("Suppressed duplicate event for payment {}", paymentId);
      metrics.recordEvent(EventOutcome.DUPLICATE_SUPPRESSED);
      return;
    }

    long now = context.currentSystemTimeMs();
    notified.put(paymentId, now);
    NotificationEvent notification = Notifications.from(event, Instant.ofEpochMilli(now));
    context.forward(
        record
            .withValue(notification.toByteArray())
            .withHeaders(withEventType(record.headers(), NOTIFICATION_EVENT_TYPE)),
        DeduplicationTopology.NOTIFICATIONS_SINK);
    metrics.recordEvent(EventOutcome.NOTIFIED);
  }

  private void deadLetter(Record<String, byte[]> record, Rejection rejection) {
    Headers headers = new RecordHeaders(record.headers().toArray());
    headers.remove(DeadLetters.REASON).remove(DeadLetters.ERROR);
    headers.add(DeadLetters.REASON, utf8(rejection.reason().code()));
    headers.add(DeadLetters.ERROR, utf8(rejection.detail()));
    Optional<RecordMetadata> origin = context.recordMetadata();
    origin.ifPresent(
        metadata -> {
          headers.add(DeadLetters.ORIGINAL_TOPIC, utf8(metadata.topic()));
          headers.add(DeadLetters.ORIGINAL_PARTITION, utf8(String.valueOf(metadata.partition())));
          headers.add(DeadLetters.ORIGINAL_OFFSET, utf8(String.valueOf(metadata.offset())));
        });

    log.warn(
        "Dead-lettered payment event at {}: {} ({})",
        origin.map(m -> m.topic() + "-" + m.partition() + "@" + m.offset()).orElse("unknown"),
        rejection.reason().code(),
        rejection.detail());
    context.forward(record.withHeaders(headers), DeduplicationTopology.DEAD_LETTER_SINK);
    metrics.recordEvent(EventOutcome.DEAD_LETTERED);
  }

  /**
   * Deletes ids first seen longer ago than the retention.
   *
   * <p>A full scan, collecting keys before deleting so the iterator is never invalidated. Linear in
   * the store size, which is bounded by the retention: at a million payments a day, eight days is
   * eight million small entries per hourly scan, spread across the partitions. Beyond that, a
   * windowed store whose segments expire as a whole would avoid the scan — see the design review.
   */
  private void purgeExpired(long now) {
    long cutoff = now - retention.toMillis();
    List<String> expired = new ArrayList<>();
    try (KeyValueIterator<String, Long> entries = notified.all()) {
      while (entries.hasNext()) {
        KeyValue<String, Long> entry = entries.next();
        if (entry.value < cutoff) {
          expired.add(entry.key);
        }
      }
    }
    expired.forEach(notified::delete);
    if (!expired.isEmpty()) {
      log.info("Purged {} expired payment ids from the deduplication store", expired.size());
      metrics.recordPurged(expired.size());
    }
  }

  private static Headers withEventType(Headers original, String eventType) {
    Headers headers = new RecordHeaders(original.toArray());
    headers.remove(EVENT_TYPE_HEADER);
    headers.add(EVENT_TYPE_HEADER, utf8(eventType));
    return headers;
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
