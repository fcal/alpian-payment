package com.alpian.payment.outbox;

import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.OutboxRepository;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
public class OutboxConfiguration {

  /**
   * The relay, when enabled. A property rather than a profile so that a deployment can run the API
   * and the relay as separate processes simply by toggling it, if they ever need to scale apart.
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "payment.outbox",
      name = "relay-enabled",
      havingValue = "true",
      matchIfMissing = true)
  public OutboxRelay outboxRelay(
      OutboxRepository outbox,
      KafkaTemplate<String, byte[]> kafka,
      PlatformTransactionManager transactionManager,
      PaymentMetrics metrics,
      OutboxProperties properties,
      Clock clock) {
    return new OutboxRelay(
        outbox, kafka, new TransactionTemplate(transactionManager), metrics, properties, clock);
  }

  /**
   * Schedules the relay from the bound {@link OutboxProperties#pollInterval()}.
   *
   * <p>Programmatic rather than {@code @Scheduled(fixedDelayString =
   * "${payment.outbox.poll-interval}")} for a reason that bit: the annotation re-reads the raw
   * property with its own parser, which accepts milliseconds or ISO-8601 but not {@code 500ms}. The
   * configuration binder had already parsed {@code 500ms} correctly into a {@link
   * java.time.Duration}, so the same key was read by two parsers with different rules, and the
   * stricter one failed the application at startup. Scheduling from the bound value leaves one
   * parser and one validated value.
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "payment.outbox",
      name = "relay-enabled",
      havingValue = "true",
      matchIfMissing = true)
  public SchedulingConfigurer outboxRelaySchedule(OutboxRelay relay, OutboxProperties properties) {
    return registrar -> registrar.addFixedDelayTask(relay::poll, properties.pollInterval());
  }
}
