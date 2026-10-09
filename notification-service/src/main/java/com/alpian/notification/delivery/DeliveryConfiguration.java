package com.alpian.notification.delivery;

import com.alpian.notification.config.NotificationProperties;
import com.alpian.notification.observability.NotificationMetrics;
import com.alpian.notification.observability.NotificationMetrics.DeliveryOutcome;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/** Retry and dead-letter policy for notification delivery. */
@Configuration(proxyBeanMethods = false)
class DeliveryConfiguration {

  /**
   * Retries a failed delivery with exponential backoff, then publishes the record, unchanged, to
   * the delivery dead-letter topic and moves on, so one undeliverable notification does not block
   * the partition behind it. Picked up by Spring Boot's listener container factory.
   *
   * <p>An undecodable record is dead-lettered at once: retrying cannot change its bytes.
   */
  @Bean
  DefaultErrorHandler deliveryErrorHandler(
      NotificationProperties properties, KafkaOperations<?, ?> kafka, NotificationMetrics metrics) {
    NotificationProperties.Delivery delivery = properties.delivery();
    DeadLetterPublishingRecoverer publisher =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, exception) ->
                new TopicPartition(delivery.deadLetterTopic(), record.partition()));

    ExponentialBackOff backOff = new ExponentialBackOff(delivery.initialBackoff().toMillis(), 2.0);
    backOff.setMaxInterval(delivery.maxBackoff().toMillis());
    backOff.setMaxAttempts(delivery.maxAttempts() - 1);

    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            (record, exception) -> {
              publisher.accept(record, exception);
              metrics.recordDelivery(DeliveryOutcome.DEAD_LETTERED);
            },
            backOff);
    handler.addNotRetryableExceptions(InvalidProtocolBufferException.class);
    return handler;
  }
}
