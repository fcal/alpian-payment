package com.alpian.notification.streams;

import com.alpian.notification.config.NotificationProperties;
import com.alpian.notification.observability.NotificationMetrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * Runs the deduplication topology under Spring's lifecycle, which also binds Kafka Streams client
 * metrics to Micrometer.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
class StreamsConfiguration {

  private static final Logger log = LoggerFactory.getLogger(StreamsConfiguration.class);

  @Bean
  StreamsBuilderFactoryBeanConfigurer deduplicationTopology(
      NotificationProperties properties, NotificationMetrics metrics) {
    return factoryBean -> {
      factoryBean.setInfrastructureCustomizer(
          new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureTopology(Topology topology) {
              DeduplicationTopology.addTo(topology, properties, metrics);
            }
          });
      // An exception escaping the processor is a bug or an unwritable output, not a bad record —
      // those are dead-lettered. Retrying would fail the same way on the same record, so the
      // client shuts down, liveness fails, and the instance is replaced; the uncommitted record is
      // reprocessed by whichever instance takes the partition over.
      factoryBean.setStreamsUncaughtExceptionHandler(
          exception -> {
            log.error("Kafka Streams thread failed; shutting the client down", exception);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
    };
  }

  /**
   * Reports whether the Streams client is processing. Wired into the liveness group: a client that
   * has shut down does not recover by itself. A broker outage leaves it {@code RUNNING} while it
   * retries, so it does not trip this.
   */
  @Bean
  HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
    return () -> {
      KafkaStreams streams = factoryBean.getKafkaStreams();
      if (streams == null) {
        return Health.down().withDetail("state", "NOT_STARTED").build();
      }
      KafkaStreams.State state = streams.state();
      Health.Builder health = state.isRunningOrRebalancing() ? Health.up() : Health.down();
      return health.withDetail("state", state.name()).build();
    };
  }
}
