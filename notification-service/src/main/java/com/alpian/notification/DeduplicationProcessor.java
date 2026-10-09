package com.alpian.notification;

import com.alpian.payment.events.v1.PaymentEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits one notification per payment id, however many times its event arrives (the outbox delivers
 * at least once).
 *
 * <p>Under {@code exactly_once_v2} the store write, the emitted notification and the input offset
 * commit atomically, so a crash cannot notify without recording the id. The store is per partition,
 * which works because every copy of an event has the same key (the account id).
 */
final class DeduplicationProcessor implements Processor<String, byte[], String, byte[]> {

  private static final Logger log = LoggerFactory.getLogger(DeduplicationProcessor.class);

  private final MeterRegistry metrics;
  private ProcessorContext<String, byte[]> context;
  private KeyValueStore<String, Long> notified;

  DeduplicationProcessor(MeterRegistry metrics) {
    this.metrics = metrics;
  }

  @Override
  public void init(ProcessorContext<String, byte[]> context) {
    this.context = context;
    this.notified = context.getStateStore(DeduplicationTopology.STORE);
  }

  @Override
  public void process(Record<String, byte[]> record) {
    PaymentEvent event = decode(record.value());
    if (event == null) {
      log.warn("Skipping undecodable payment event with key {}", record.key());
      count("skipped");
      return;
    }
    String problem = Notifications.problem(event);
    if (problem != null) {
      log.warn("Skipping invalid event for payment {}: {}", event.getPaymentId(), problem);
      count("skipped");
      return;
    }

    if (notified.get(event.getPaymentId()) != null) {
      log.info("Suppressed duplicate event for payment {}", event.getPaymentId());
      count("duplicate");
      return;
    }

    long now = context.currentSystemTimeMs();
    notified.put(event.getPaymentId(), now);
    Record<String, byte[]> notification =
        record.withValue(Notifications.from(event, Instant.ofEpochMilli(now)).toByteArray());
    notification.headers().remove("event-type");
    notification
        .headers()
        .add("event-type", "alpian.payment.v1.NotificationEvent".getBytes(StandardCharsets.UTF_8));
    context.forward(notification);
    count("notified");
  }

  private static PaymentEvent decode(byte[] value) {
    try {
      PaymentEvent event = PaymentEvent.parseFrom(value == null ? new byte[0] : value);
      return event.getPaymentId().isEmpty() ? null : event;
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private void count(String outcome) {
    metrics.counter("notification.events", "outcome", outcome).increment();
  }
}
