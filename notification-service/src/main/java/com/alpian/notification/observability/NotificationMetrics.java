package com.alpian.notification.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Application metrics for deduplication and delivery.
 *
 * <p>Every outcome is registered at zero on startup, as in the payment service: a counter that has
 * never been incremented is absent from a scrape, and an alert on it would evaluate against a
 * series that does not exist until the first occurrence.
 *
 * <p>The deduplication counters are incremented inside the Streams processor, which is not part of
 * the exactly-once transaction: when a task aborts and re-processes a batch, the records in it are
 * counted again. The counters are therefore exact in steady state and may over-count around a crash
 * or rebalance. The notifications themselves are not affected.
 */
@Component
public class NotificationMetrics {

  private static final String EVENTS = "notification.events";
  private static final String PURGED = "notification.deduplication.purged";
  private static final String DELIVERIES = "notification.deliveries";

  /** What the deduplication step did with a payment event. */
  public enum EventOutcome {
    /** First sighting of the payment id: a notification was emitted. */
    NOTIFIED,
    /** The payment id had already been notified; the event was dropped. */
    DUPLICATE_SUPPRESSED,
    /** The event could not be turned into a notification and went to the dead-letter topic. */
    DEAD_LETTERED;

    String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /** What happened to one attempt to deliver a notification. */
  public enum DeliveryOutcome {
    /** The notification was handed to the provider. */
    SENT,
    /** The attempt failed and will be retried, or dead-lettered if it was the last. */
    FAILED,
    /** Every attempt failed; the notification went to the delivery dead-letter topic. */
    DEAD_LETTERED;

    String tag() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final Map<EventOutcome, Counter> events = new EnumMap<>(EventOutcome.class);
  private final Map<DeliveryOutcome, Counter> deliveries = new EnumMap<>(DeliveryOutcome.class);
  private final Counter purged;

  public NotificationMetrics(MeterRegistry registry) {
    for (EventOutcome outcome : EventOutcome.values()) {
      events.put(
          outcome,
          Counter.builder(EVENTS)
              .description("Payment events consumed, by what deduplication did with them")
              .tag("outcome", outcome.tag())
              .register(registry));
    }
    for (DeliveryOutcome outcome : DeliveryOutcome.values()) {
      deliveries.put(
          outcome,
          Counter.builder(DELIVERIES)
              .description("Notification delivery attempts, by outcome")
              .tag("outcome", outcome.tag())
              .register(registry));
    }
    purged =
        Counter.builder(PURGED)
            .description("Payment ids purged from the deduplication store after their retention")
            .register(registry);
  }

  public void recordEvent(EventOutcome outcome) {
    events.get(outcome).increment();
  }

  public void recordDelivery(DeliveryOutcome outcome) {
    deliveries.get(outcome).increment();
  }

  public void recordPurged(int count) {
    purged.increment(count);
  }
}
