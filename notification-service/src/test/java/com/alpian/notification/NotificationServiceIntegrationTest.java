package com.alpian.notification;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The service against a real broker. To assert that something happened only once, each test ends
 * with a marker event on the same account: it is processed after everything before it, so once it
 * is delivered, any duplicate would have been too.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "spring.docker.compose.enabled=false",
      "spring.kafka.streams.properties.num.standby.replicas=0",
      "notification.delivery.max-attempts=3",
      "notification.delivery.initial-backoff=100ms",
      "notification.delivery.max-backoff=200ms"
    })
class NotificationServiceIntegrationTest {

  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");
  static final Path STATE_DIR;

  static {
    KAFKA.start();
    try (AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(DeduplicationTopology.INPUT_TOPIC, 3, (short) 1),
                  new NewTopic(DeduplicationTopology.OUTPUT_TOPIC, 3, (short) 1),
                  new NewTopic(KafkaConfiguration.DELIVERY_DLT, 3, (short) 1)))
          .all()
          .get();
      STATE_DIR = Files.createTempDirectory("notification-streams");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static final KafkaProducer<String, byte[]> PRODUCER =
      new KafkaProducer<>(
          Map.of(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));

  @DynamicPropertySource
  static void kafka(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.streams.state-dir", STATE_DIR::toString);
  }

  @AfterAll
  static void closeProducer() {
    PRODUCER.close();
  }

  @TestConfiguration
  static class Config {
    @Bean
    @Primary
    RecordingSender recordingSender() {
      return new RecordingSender();
    }
  }

  /** Records deliveries, and fails every attempt for the payment ids it is told to. */
  static class RecordingSender implements NotificationSender {
    final Queue<String> delivered = new ConcurrentLinkedQueue<>();
    final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    final Set<String> failing = ConcurrentHashMap.newKeySet();

    @Override
    public void send(NotificationEvent notification) {
      String id = notification.getPaymentId();
      attempts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
      if (failing.contains(id)) {
        throw new IllegalStateException("provider rejected " + id);
      }
      delivered.add(id);
    }

    long deliveries(String id) {
      return delivered.stream().filter(id::equals).count();
    }
  }

  @Autowired RecordingSender sender;
  @Autowired StreamsBuilderFactoryBean streams;

  @Autowired
  @Qualifier("kafkaStreamsHealthIndicator")
  HealthIndicator streamsHealth;

  @Test
  @DisplayName("an event received three times is delivered once")
  void duplicatesAreDeliveredOnce() {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();

    send(event);
    send(event);
    send(event);
    awaitDelivered(sendMarker(account));

    assertThat(sender.deliveries(event.getPaymentId())).isEqualTo(1);
  }

  @Test
  @DisplayName("an undeliverable notification is retried, then dead-lettered")
  void undeliverableIsRetriedThenDeadLettered() throws Exception {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();
    sender.failing.add(event.getPaymentId());

    send(event);
    awaitDelivered(sendMarker(account));

    assertThat(sender.attempts.get(event.getPaymentId())).hasValue(3);
    ConsumerRecord<String, byte[]> dead = awaitDeadLetter(account);
    assertThat(NotificationEvent.parseFrom(dead.value()).getPaymentId())
        .isEqualTo(event.getPaymentId());
  }

  @Test
  @DisplayName("an invalid event is skipped, and Streams keeps processing and reports healthy")
  void invalidEventDoesNotStopProcessing() {
    String account = UUID.randomUUID().toString();
    PaymentEvent invalid = Events.completed(account).clearAmount().build();

    send(invalid);
    awaitDelivered(sendMarker(account));

    assertThat(sender.attempts).doesNotContainKey(invalid.getPaymentId());
    assertThat(streams.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(streamsHealth.health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  @DisplayName("deduplication survives a restart with the local state wiped")
  void deduplicationSurvivesLosingLocalState() {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();
    send(event);
    awaitDelivered(sendMarker(account));

    // As if the instance were replaced: the store can only come back from its changelog topic.
    KafkaStreams client = streams.getKafkaStreams();
    streams.stop();
    client.cleanUp();
    streams.start();
    await()
        .atMost(60, SECONDS)
        .until(() -> streams.getKafkaStreams().state() == KafkaStreams.State.RUNNING);

    send(event);
    awaitDelivered(sendMarker(account));

    assertThat(sender.deliveries(event.getPaymentId())).isEqualTo(1);
  }

  private static void send(PaymentEvent event) {
    PRODUCER.send(
        new ProducerRecord<>(
            DeduplicationTopology.INPUT_TOPIC, event.getAccountId(), event.toByteArray()));
  }

  private static PaymentEvent sendMarker(String account) {
    PaymentEvent marker = Events.completed(account).setBeneficiaryName("Marker").build();
    send(marker);
    PRODUCER.flush();
    return marker;
  }

  private void awaitDelivered(PaymentEvent event) {
    await().atMost(60, SECONDS).until(() -> sender.deliveries(event.getPaymentId()) > 0);
  }

  private static ConsumerRecord<String, byte[]> awaitDeadLetter(String account) {
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
      consumer.subscribe(List.of(KafkaConfiguration.DELIVERY_DLT));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          if (account.equals(record.key())) {
            return record;
          }
        }
      }
    }
    throw new AssertionError("no dead letter for account " + account);
  }
}
