package com.alpian.notification;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class KafkaConfiguration {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

  static final String DELIVERY_DLT = "notification-events-dlt";

  @Bean
  KStream<String, byte[]> deduplication(StreamsBuilder builder, MeterRegistry metrics) {
    return DeduplicationTopology.build(builder, metrics);
  }

  /**
   * An exception escaping the topology is a bug or an unwritable output, not a bad record (those
   * are skipped). Retrying would fail the same way, so the client shuts down and the health check
   * below fails, getting the instance replaced. The record is uncommitted and gets reprocessed.
   */
  @Bean
  StreamsBuilderFactoryBeanConfigurer shutDownOnUncaughtException() {
    return factoryBean ->
        factoryBean.setStreamsUncaughtExceptionHandler(
            e -> {
              log.error("Kafka Streams thread failed; shutting the client down", e);
              return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
  }

  /**
   * Whether Streams is processing. In the liveness and readiness groups: a stopped client never
   * recovers by itself, and "ready" must not mean "started but not processing". A broker outage
   * keeps the client RUNNING while it retries, so it does not trip this.
   */
  @Bean
  HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
    return () -> {
      KafkaStreams streams = factoryBean.getKafkaStreams();
      if (streams == null) {
        return Health.down().withDetail("state", "NOT_STARTED").build();
      }
      KafkaStreams.State state = streams.state();
      return (state.isRunningOrRebalancing() ? Health.up() : Health.down())
          .withDetail("state", state.name())
          .build();
    };
  }

  /**
   * Retries a failed delivery with exponential backoff, then moves the record to the dead-letter
   * topic so it does not block the partition. An undecodable record is dead-lettered at once.
   */
  @Bean
  DefaultErrorHandler deliveryErrorHandler(
      KafkaOperations<?, ?> kafka,
      MeterRegistry metrics,
      @Value("${notification.delivery.max-attempts:5}") int maxAttempts,
      @Value("${notification.delivery.initial-backoff:1s}") Duration initialBackoff,
      @Value("${notification.delivery.max-backoff:30s}") Duration maxBackoff) {
    DeadLetterPublishingRecoverer deadLetter =
        new DeadLetterPublishingRecoverer(
            kafka, (record, e) -> new TopicPartition(DELIVERY_DLT, record.partition()));
    ExponentialBackOff backOff = new ExponentialBackOff(initialBackoff.toMillis(), 2.0);
    backOff.setMaxInterval(maxBackoff.toMillis());
    backOff.setMaxAttempts(maxAttempts - 1);

    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            (record, e) -> {
              deadLetter.accept(record, e);
              metrics.counter("notification.deliveries.dead.lettered").increment();
            },
            backOff);
    handler.addNotRetryableExceptions(InvalidProtocolBufferException.class);
    return handler;
  }
}
