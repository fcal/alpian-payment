package com.alpian.notification.config;

import java.time.Duration;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Topics and tuning for the notification service.
 *
 * @param inputTopic payment events to deduplicate
 * @param outputTopic deduplicated notifications
 * @param deadLetterTopic payment events that cannot be turned into a notification
 * @param deduplicationRetention how long a payment id is remembered as notified; must exceed the
 *     input topic's retention
 * @param purgeInterval how often expired ids are purged from the store
 * @param delivery retry and dead-letter settings for delivering notifications
 */
@ConfigurationProperties("notification")
public record NotificationProperties(
    @DefaultValue("payment-events") String inputTopic,
    @DefaultValue("notification-events") String outputTopic,
    @DefaultValue("payment-events-dlt") String deadLetterTopic,
    @DefaultValue("8d") Duration deduplicationRetention,
    @DefaultValue("1h") Duration purgeInterval,
    @DefaultValue Delivery delivery) {

  public NotificationProperties {
    Objects.requireNonNull(inputTopic, "inputTopic");
    Objects.requireNonNull(outputTopic, "outputTopic");
    Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
    requirePositive(deduplicationRetention, "deduplicationRetention");
    requirePositive(purgeInterval, "purgeInterval");
    Objects.requireNonNull(delivery, "delivery");
  }

  /**
   * @param deadLetterTopic notifications whose delivery failed every attempt
   * @param maxAttempts delivery attempts, including the first, before dead-lettering
   * @param initialBackoff delay before the first retry; doubles on each subsequent one
   * @param maxBackoff cap on the delay between retries
   */
  public record Delivery(
      @DefaultValue("notification-events-dlt") String deadLetterTopic,
      @DefaultValue("5") int maxAttempts,
      @DefaultValue("1s") Duration initialBackoff,
      @DefaultValue("30s") Duration maxBackoff) {

    public Delivery {
      Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("maxAttempts must be at least 1");
      }
      requirePositive(initialBackoff, "initialBackoff");
      requirePositive(maxBackoff, "maxBackoff");
    }
  }

  private static void requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }
}
