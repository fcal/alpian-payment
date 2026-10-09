package com.alpian.payment.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param maxAttempts attempts before a message failing with a non-retriable error is parked.
 *     Retriable errors (broker unreachable) are retried forever.
 * @param maxBackoff cap on the retry delay, which starts at 1s and doubles
 * @param sendTimeout how long to wait for a batch to be acknowledged
 */
@ConfigurationProperties("payment.outbox")
public record OutboxProperties(
    @DefaultValue("payment-events") String topic,
    @DefaultValue("100") int batchSize,
    @DefaultValue("5") int maxAttempts,
    @DefaultValue("1m") Duration maxBackoff,
    @DefaultValue("15s") Duration sendTimeout) {

  Duration backoff(int failures) {
    Duration delay = Duration.ofSeconds(1L << Math.min(failures, 20));
    return delay.compareTo(maxBackoff) > 0 ? maxBackoff : delay;
  }
}
