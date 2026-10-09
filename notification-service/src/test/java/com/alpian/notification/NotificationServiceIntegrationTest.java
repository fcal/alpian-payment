package com.alpian.notification;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.alpian.notification.delivery.NotificationSender;
import com.alpian.notification.streams.DeadLetters;
import com.alpian.notification.support.Events;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
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
 * The service as Spring assembles it, against a real broker: Streams under exactly-once, the store
 * and its changelog, the delivery listener and both dead-letter topics.
 *
 * <p>Asserting that something happened <em>once</em> needs a point after which a second occurrence
 * would already have been seen. Each scenario therefore ends with a marker event on the same
 * account: same key, same partition, processed and delivered in order after everything before it.
 * Once the marker is delivered, any duplicate would have been too.
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
                  new NewTopic("payment-events", 3, (short) 1),
                  new NewTopic("notification-events", 3, (short) 1),
                  new NewTopic("payment-events-dlt", 3, (short) 1),
                  new NewTopic("notification-events-dlt", 3, (short) 1)))
          .all()
          .get();
      STATE_DIR = Files.createTempDirectory("notification-streams");
    } catch (Exception e) {
      throw new IllegalStateException("Could not prepare the broker", e);
    }
  }

  @DynamicPropertySource
  static void kafka(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.streams.state-dir", STATE_DIR::toString);
  }

  @TestConfiguration
  static class RecordingSenderConfiguration {
    @Bean
    @Primary
    RecordingSender recordingSender() {
      return new RecordingSender();
    }
  }

  /** Records deliveries, and fails every attempt for payment ids it is told to. */
  static class RecordingSender implements NotificationSender {
    final Queue<NotificationEvent> delivered = new ConcurrentLinkedQueue<>();
    final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    final Set<String> undeliverable = ConcurrentHashMap.newKeySet();

    @Override
    public void send(NotificationEvent notification) {
      attempts
          .computeIfAbsent(notification.getPaymentId(), id -> new AtomicInteger())
          .incrementAndGet();
      if (undeliverable.contains(notification.getPaymentId())) {
        throw new IllegalStateException("provider rejected " + notification.getPaymentId());
      }
      delivered.add(notification);
    }

    long deliveriesOf(String paymentId) {
      return delivered.stream().filter(n -> n.getPaymentId().equals(paymentId)).count();
    }
  }

  private static final KafkaProducer<String, byte[]> PRODUCER =
      new KafkaProducer<>(
          Map.of(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));

  @AfterAll
  static void closeProducer() {
    PRODUCER.close();
  }

  @Autowired RecordingSender sender;
  @Autowired MeterRegistry registry;
  @Autowired StreamsBuilderFactoryBean streams;

  @Autowired
  @Qualifier("kafkaStreamsHealthIndicator")
  HealthIndicator streamsHealth;

  @Test
  @DisplayName("an event delivered three times produces one notification and one delivery")
  void duplicatesAreDeliveredOnce() {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();
    double suppressedBefore = counter("notification.events", "duplicate_suppressed");

    send(event);
    send(event);
    send(event);
    PaymentEvent marker = sendMarker(account);

    awaitDelivered(marker);
    assertThat(sender.deliveriesOf(event.getPaymentId())).isEqualTo(1);
    assertThat(notificationsOnTopic(account, marker))
        .containsExactly(event.getPaymentId(), marker.getPaymentId());
    assertThat(counter("notification.events", "duplicate_suppressed") - suppressedBefore)
        .isEqualTo(2);
    assertThat(streamsHealth.health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  @DisplayName("an undecodable event is dead-lettered unchanged and does not block the partition")
  void undecodableEventIsDeadLettered() {
    String account = UUID.randomUUID().toString();
    byte[] garbage = ("garbage " + account).getBytes(StandardCharsets.UTF_8);

    PRODUCER.send(new ProducerRecord<>("payment-events", account, garbage));
    PaymentEvent marker = sendMarker(account);

    awaitDelivered(marker);
    ConsumerRecord<String, byte[]> dead =
        awaitRecord("payment-events-dlt", r -> account.equals(r.key()));
    assertThat(dead.value()).isEqualTo(garbage);
    assertThat(header(dead, DeadLetters.REASON)).isEqualTo("undecodable");
    assertThat(header(dead, DeadLetters.ORIGINAL_TOPIC)).isEqualTo("payment-events");
  }

  @Test
  @DisplayName("a notification that cannot be delivered is retried, then dead-lettered")
  void undeliverableNotificationIsRetriedThenDeadLettered() throws Exception {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();
    sender.undeliverable.add(event.getPaymentId());
    double deadLetteredBefore = counter("notification.deliveries", "dead_lettered");

    send(event);
    PaymentEvent marker = sendMarker(account);

    awaitDelivered(marker);
    assertThat(sender.attempts.get(event.getPaymentId())).hasValue(3);
    ConsumerRecord<String, byte[]> dead =
        awaitRecord("notification-events-dlt", r -> account.equals(r.key()));
    assertThat(NotificationEvent.parseFrom(dead.value()).getPaymentId())
        .isEqualTo(event.getPaymentId());
    assertThat(counter("notification.deliveries", "dead_lettered") - deadLetteredBefore)
        .isEqualTo(1);
  }

  @Test
  @DisplayName("deduplication survives a restart with the local state wiped")
  void deduplicationSurvivesLosingLocalState() {
    String account = UUID.randomUUID().toString();
    PaymentEvent event = Events.completed(account).build();
    send(event);
    awaitDelivered(sendMarker(account));

    // As if the instance were replaced: the RocksDB files are gone, and the store can only come
    // back from its changelog topic.
    KafkaStreams client = streams.getKafkaStreams();
    streams.stop();
    assertThat(streamsHealth.health().getStatus()).isEqualTo(Status.DOWN);
    client.cleanUp();
    streams.start();
    await().atMost(60, SECONDS).until(() -> streamsHealth.health().getStatus().equals(Status.UP));

    send(event);
    PaymentEvent marker = sendMarker(account);

    awaitDelivered(marker);
    assertThat(sender.deliveriesOf(event.getPaymentId())).isEqualTo(1);
  }

  private void send(PaymentEvent event) {
    PRODUCER.send(
        new ProducerRecord<>("payment-events", event.getAccountId(), event.toByteArray()));
  }

  private PaymentEvent sendMarker(String account) {
    PaymentEvent marker = Events.completed(account).setBeneficiaryName("Marker").build();
    send(marker);
    PRODUCER.flush();
    return marker;
  }

  private void awaitDelivered(PaymentEvent event) {
    await().atMost(60, SECONDS).until(() -> sender.deliveriesOf(event.getPaymentId()) > 0);
  }

  /**
   * Payment ids of the committed notifications for {@code account}, up to and including {@code
   * marker}.
   */
  private List<String> notificationsOnTopic(String account, PaymentEvent marker) {
    List<String> ids = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer = consumer()) {
      consumer.subscribe(List.of("notification-events"));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          if (account.equals(record.key())) {
            String id = NotificationEvent.parseFrom(record.value()).getPaymentId();
            ids.add(id);
            if (id.equals(marker.getPaymentId())) {
              return ids;
            }
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
    throw new AssertionError("marker not seen on notification-events; saw " + ids);
  }

  private ConsumerRecord<String, byte[]> awaitRecord(
      String topic, Predicate<ConsumerRecord<String, byte[]>> matching) {
    try (KafkaConsumer<String, byte[]> consumer = consumer()) {
      consumer.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          if (matching.test(record)) {
            return record;
          }
        }
      }
    }
    throw new AssertionError("no matching record on " + topic);
  }

  private static KafkaConsumer<String, byte[]> consumer() {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "test-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            // Only what committed transactions wrote: aborted output must not count as a duplicate.
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            "read_committed",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class));
  }

  private double counter(String name, String outcome) {
    return registry.get(name).tag("outcome", outcome).counter().count();
  }

  private static String header(ConsumerRecord<String, byte[]> record, String key) {
    var header = record.headers().lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }
}
