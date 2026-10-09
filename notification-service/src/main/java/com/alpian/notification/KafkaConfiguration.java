package com.alpian.notification;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class KafkaConfiguration {

  static final String DELIVERY_DLT = "notification-events-dlt";

  @Bean
  KStream<String, byte[]> deduplication(StreamsBuilder builder, MeterRegistry metrics) {
    return DeduplicationTopology.build(builder, metrics);
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
