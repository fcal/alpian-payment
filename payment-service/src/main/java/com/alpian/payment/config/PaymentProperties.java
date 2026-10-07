package com.alpian.payment.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunables for the payment path.
 *
 * @param lockTimeout how long a payment waits for the per-account row lock before being rejected.
 *     Bounded deliberately: a waiting transaction holds its connection for the duration of the
 *     wait, so an unbounded wait lets a burst on one account exhaust the pool and stall payments on
 *     unrelated accounts. Exceeding it yields a retryable rejection rather than a hung request.
 */
@ConfigurationProperties(prefix = "payment")
public record PaymentProperties(Duration lockTimeout) {

  public PaymentProperties {
    if (lockTimeout == null) {
      lockTimeout = Duration.ofSeconds(3);
    }
    if (lockTimeout.isNegative() || lockTimeout.isZero()) {
      throw new IllegalArgumentException("payment.lock-timeout must be positive: " + lockTimeout);
    }
  }
}
