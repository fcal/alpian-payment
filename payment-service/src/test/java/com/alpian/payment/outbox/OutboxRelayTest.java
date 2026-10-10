package com.alpian.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.repository.OutboxRepository;
import com.alpian.payment.service.PaymentService;
import com.alpian.payment.support.ApplicationTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The relay against real Postgres and Kafka. It is driven by hand with {@code publishBatch()}; a
 * broker outage is a producer pointed at a port nothing listens on.
 */
class OutboxRelayTest extends ApplicationTestBase {

  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  static {
    KAFKA.start();
  }

  private static final int MAX_ATTEMPTS = 3;

  @Autowired PaymentService service;
  @Autowired OutboxRepository outbox;
  @Autowired TransactionTemplate transactions;

  private final List<DefaultKafkaProducerFactory<String, byte[]>> producers = new ArrayList<>();
  private String topic;
  private Fixture f;

  @BeforeEach
  void setUp() throws Exception {
    // Earlier tests leave unpublished rows behind; park them so only this test's rows are due.
    jdbc.sql("UPDATE outbox SET parked_at = now() WHERE published_at IS NULL").update();
    topic = "payment-events-" + UUID.randomUUID();
    try (AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
    }
    f = givenAccount("1000.00", "CHF");
  }

  @AfterEach
  void closeProducers() {
    producers.forEach(DefaultKafkaProducerFactory::destroy);
  }

  @Test
  @DisplayName("a committed payment is published once, keyed by account, and marked published")
  void publishes() throws Exception {
    UUID paymentId = service.submit(f.request("250.50", "key-1")).payment().id();

    assertThat(relay(broker(Map.of())).publishBatch()).isEqualTo(1);

    ConsumerRecord<String, byte[]> record = consume(1).get(0);
    assertThat(record.key()).isEqualTo(f.account().toString());
    assertThat(header(record, OutboxRelay.HEADER_PAYMENT_ID)).isEqualTo(paymentId.toString());
    PaymentEvent event = PaymentEvent.parseFrom(record.value());
    assertThat(event.getPaymentId()).isEqualTo(paymentId.toString());
    assertThat(event.getAmount().getAmount()).isEqualTo("250.5000");
    assertThat(outbox.countPending()).isZero();
  }

  @Test
  @DisplayName("with Kafka down, the event is kept and retried, never parked, then delivered")
  void survivesAnOutage() {
    service.submit(f.request("100.00", "during-outage"));
    OutboxRelay down = relay(unreachableBroker());

    for (int i = 0; i < MAX_ATTEMPTS + 2; i++) {
      down.publishBatch();
      makeDue();
    }

    assertThat(row("attempts")).isEqualTo(String.valueOf(MAX_ATTEMPTS + 2));
    assertThat(row("parked_at")).isNull();
    assertThat(row("last_error")).contains("TimeoutException");

    relay(broker(Map.of())).publishBatch();
    assertThat(consume(1)).hasSize(1);
    assertThat(row("published_at")).isNotNull();
  }

  @Test
  @DisplayName("a non-retriable failure is parked after the attempt limit")
  void parksAPoisonMessage() {
    service.submit(f.request("10.00", "poison"));
    // Every record exceeds the size limit: RecordTooLargeException, which retrying cannot fix.
    OutboxRelay relay = relay(broker(Map.of(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1)));

    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      relay.publishBatch();
      makeDue();
    }

    assertThat(row("parked_at")).isNotNull();
    assertThat(row("last_error")).contains("RecordTooLargeException");
    assertThat(relay.publishBatch()).isZero();
  }

  @Test
  @DisplayName("two relays running concurrently publish every event exactly once")
  void concurrentRelaysDoNotDuplicate() throws Exception {
    int events = 200;
    transactions.executeWithoutResult(
        status -> {
          for (int i = 0; i < events; i++) {
            outbox.enqueue(UUID.randomUUID(), f.account().toString(), new byte[] {1});
          }
        });
    OutboxRelay first = relay(broker(Map.of()));
    OutboxRelay second = relay(broker(Map.of()));

    CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> drain(first)),
            CompletableFuture.runAsync(() -> drain(second)))
        .get();

    List<ConsumerRecord<String, byte[]>> records = consume(events);
    Set<String> distinct = new HashSet<>();
    records.forEach(r -> distinct.add(header(r, OutboxRelay.HEADER_PAYMENT_ID)));
    assertThat(records).hasSize(events);
    assertThat(distinct).hasSize(events);
  }

  private OutboxRelay relay(KafkaTemplate<String, byte[]> kafka) {
    OutboxProperties properties =
        new OutboxProperties(
            topic, 20, MAX_ATTEMPTS, Duration.ofMinutes(1), Duration.ofSeconds(10));
    return new OutboxRelay(outbox, kafka, transactions, properties, new SimpleMeterRegistry());
  }

  private static void drain(OutboxRelay relay) {
    while (relay.publishBatch() > 0) {
      // until this relay finds nothing left
    }
  }

  /** Skips the backoff, as if it had elapsed. */
  private void makeDue() {
    jdbc.sql("UPDATE outbox SET next_attempt_at = now() WHERE parked_at IS NULL").update();
  }

  private String row(String column) {
    return jdbc.sql("SELECT " + column + "::text FROM outbox WHERE partition_key = :account")
        .param("account", f.account().toString())
        .query(String.class)
        .single();
  }

  private KafkaTemplate<String, byte[]> broker(Map<String, Object> overrides) {
    return template(KAFKA.getBootstrapServers(), overrides);
  }

  private KafkaTemplate<String, byte[]> unreachableBroker() {
    return template(
        "localhost:1",
        Map.of(
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 500,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000));
  }

  private KafkaTemplate<String, byte[]> template(String bootstrap, Map<String, Object> overrides) {
    Map<String, Object> config = new HashMap<>(overrides);
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    DefaultKafkaProducerFactory<String, byte[]> factory = new DefaultKafkaProducerFactory<>(config);
    producers.add(factory);
    return new KafkaTemplate<>(factory);
  }

  private List<ConsumerRecord<String, byte[]>> consume(int expected) {
    List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
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
      consumer.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + 20_000;
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
      }
      // Poll a little longer, so a duplicate would be seen.
      consumer.poll(Duration.ofSeconds(1)).forEach(records::add);
    }
    return records;
  }

  private static String header(ConsumerRecord<String, byte[]> record, String name) {
    return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
  }
}
