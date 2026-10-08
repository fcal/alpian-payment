package com.alpian.payment.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Outbox relay tunables.
 *
 * @param relayEnabled whether this instance runs the relay. Every replica may; {@code SKIP LOCKED}
 *     keeps them from publishing the same rows. Disabled in tests that have no broker.
 * @param pollInterval delay between polls — the floor on event latency when the backlog is empty
 * @param batchSize rows claimed per transaction
 * @param maxAttempts attempts before a message failing with a <em>non-retriable</em> error is
 *     parked. Retriable failures — an unreachable broker — never park, however long they last.
 * @param initialBackoff delay after the first failure, doubled on each subsequent one
 * @param maxBackoff ceiling on the delay, so a long outage is retried at a steady rate rather than
 *     ever more rarely, and the backlog drains promptly once the broker returns
 * @param sendTimeout how long the relay waits for one batch's acknowledgements
 */
@ConfigurationProperties(prefix = "payment.outbox")
public record OutboxProperties(
    boolean relayEnabled,
    String topic,
    Duration pollInterval,
    int batchSize,
    int maxAttempts,
    Duration initialBackoff,
    Duration maxBackoff,
    Duration sendTimeout) {

  public OutboxProperties {
    if (topic == null || topic.isBlank()) {
      topic = "payment-events";
    }
    pollInterval = pollInterval == null ? Duration.ofMillis(500) : pollInterval;
    batchSize = batchSize <= 0 ? 100 : batchSize;
    maxAttempts = maxAttempts <= 0 ? 5 : maxAttempts;
    initialBackoff = initialBackoff == null ? Duration.ofSeconds(1) : initialBackoff;
    maxBackoff = maxBackoff == null ? Duration.ofMinutes(1) : maxBackoff;
    sendTimeout = sendTimeout == null ? Duration.ofSeconds(15) : sendTimeout;
  }

  /**
   * Delay before the attempt following {@code failuresSoFar} failures: {@code initialBackoff},
   * doubling, capped at {@code maxBackoff}.
   */
  public Duration backoffAfter(int failuresSoFar) {
    // Shift capped well below overflow; the max() cap takes over long before it matters.
    int exponent = Math.min(Math.max(failuresSoFar, 0), 20);
    Duration delay = initialBackoff.multipliedBy(1L << exponent);
    return delay.compareTo(maxBackoff) > 0 ? maxBackoff : delay;
  }
}
