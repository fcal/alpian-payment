package com.alpian.notification.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.notification.observability.NotificationMetrics.DeliveryOutcome;
import com.alpian.notification.observability.NotificationMetrics.EventOutcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NotificationMetricsTest {

  @Test
  @DisplayName("every outcome series exists at zero before anything happens")
  void preRegistered() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new NotificationMetrics(registry);

    for (EventOutcome outcome : EventOutcome.values()) {
      assertThat(
              registry.get("notification.events").tag("outcome", outcome.tag()).counter().count())
          .isZero();
    }
    for (DeliveryOutcome outcome : DeliveryOutcome.values()) {
      assertThat(
              registry
                  .get("notification.deliveries")
                  .tag("outcome", outcome.tag())
                  .counter()
                  .count())
          .isZero();
    }
    assertThat(registry.get("notification.deduplication.purged").counter().count()).isZero();
  }

  @Test
  @DisplayName("outcome tags are the lower-case names alerts refer to")
  void tags() {
    assertThat(EventOutcome.DUPLICATE_SUPPRESSED.tag()).isEqualTo("duplicate_suppressed");
    assertThat(DeliveryOutcome.DEAD_LETTERED.tag()).isEqualTo("dead_lettered");
  }
}
