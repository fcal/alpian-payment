package com.alpian.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxPropertiesTest {

  private final OutboxProperties properties =
      new OutboxProperties(
          true, "payment-events", null, 0, 0, Duration.ofSeconds(1), Duration.ofMinutes(1), null);

  @Test
  void doublesFromTheInitialDelay() {
    assertThat(properties.backoffAfter(0)).isEqualTo(Duration.ofSeconds(1));
    assertThat(properties.backoffAfter(1)).isEqualTo(Duration.ofSeconds(2));
    assertThat(properties.backoffAfter(3)).isEqualTo(Duration.ofSeconds(8));
  }

  @Test
  @DisplayName("the delay is capped, so a long outage is retried steadily and drains promptly")
  void capsTheDelay() {
    assertThat(properties.backoffAfter(6)).isEqualTo(Duration.ofMinutes(1));
    // A message that has failed through a very long outage must not overflow the shift.
    assertThat(properties.backoffAfter(10_000)).isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  void suppliesDefaults() {
    assertThat(properties.batchSize()).isEqualTo(100);
    assertThat(properties.maxAttempts()).isEqualTo(5);
    assertThat(properties.pollInterval()).isEqualTo(Duration.ofMillis(500));
  }
}
