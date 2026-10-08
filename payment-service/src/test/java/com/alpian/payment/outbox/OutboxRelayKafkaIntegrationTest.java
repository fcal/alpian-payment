package com.alpian.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.OutboxMessage;
import com.alpian.payment.repository.postgres.PostgresTestBase;
import com.alpian.payment.service.PaymentRequest;
import com.alpian.payment.service.PaymentService;
import com.alpian.payment.support.MutableClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
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
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The relay against a real broker and a real database.
 *
 * <p>The relay is driven by calling {@link OutboxRelay#drain()} directly rather than through its
 * schedule, and time is a {@link MutableClock}: every scenario is then deterministic, with no
 * sleeping and no waiting on a poll interval.
 *
 * <p>A broker outage is simulated with a producer pointed at an address nothing listens on, rather
 * than by stopping the container. Restarting a container moves its mapped port, which would make
 * "Kafka comes back" impossible to express; two producers make it a one-liner.
 */
class OutboxRelayKafkaIntegrationTest extends PostgresTestBase {

  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  static {
    KAFKA.start();
  }

  private static final Instant START = Instant.parse("2026-10-08T09:00:00Z");
  private static final int MAX_ATTEMPTS = 3;

  private final List<DefaultKafkaProducerFactory<String, byte[]>> factories = new ArrayList<>();

  private MutableClock clock;
  private MeterRegistry registry;
  private PaymentMetrics metrics;
  private PaymentService service;
  private String topic;
  private UserId owner;
  private AccountId account;

  @BeforeEach
  void setUp() throws Exception {
    clock = new MutableClock(START);
    registry = new SimpleMeterRegistry();
    metrics = new PaymentMetrics(registry, clock);
    service = paymentService(clock, registry);

    // A topic per test, so no test reads another's records.
    topic = "payment-events-" + UUID.randomUUID();
    try (AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
    }

    owner = new UserId(UUID.randomUUID());
    account = new AccountId(UUID.randomUUID());
    jdbc.sql("INSERT INTO app_user (id, name) VALUES (:id, 'Test User')")
        .param("id", owner.value())
        .update();
    accounts.save(new Account(account, owner, Money.of("1000.00", "CHF"), START, START));
  }

  @AfterEach
  void closeProducers() {
    factories.forEach(DefaultKafkaProducerFactory::destroy);
  }

  @Test
  @DisplayName("a committed payment is published once, keyed by account, and marked published")
  void publishesACommittedPayment() throws Exception {
    PaymentResult result = pay("250.50", "key-1");

    int claimed = relay(liveBroker()).drain();

    assertThat(claimed).isEqualTo(1);
    List<ConsumerRecord<String, byte[]>> records = consume(1);
    ConsumerRecord<String, byte[]> record = records.get(0);

    assertThat(record.key()).as("partition key").isEqualTo(account.toString());
    assertThat(header(record, OutboxRelay.HEADER_PAYMENT_ID))
        .isEqualTo(result.journalEntry().orElseThrow().id().toString());
    assertThat(header(record, OutboxRelay.HEADER_EVENT_TYPE)).isEqualTo(PaymentEvents.EVENT_TYPE);

    PaymentEvent event = PaymentEvent.parseFrom(record.value());
    assertThat(event.getPaymentId()).isEqualTo(result.journalEntry().orElseThrow().id().toString());
    assertThat(event.getUserId()).isEqualTo(owner.toString());
    assertThat(event.getAmount().getAmount()).isEqualTo("250.5000");

    assertThat(row().publishedAt()).isNotNull();
    assertThat(gauge("payment.outbox.pending")).isZero();
    assertThat(registry.find("payment.outbox.published").counter().count()).isEqualTo(1);
  }

  @Test
  @DisplayName("with Kafka down, payments still succeed and their events are delivered on recovery")
  void survivesAKafkaOutage() throws Exception {
    // The fault-tolerance property the outbox exists for. The payment path never touches Kafka,
    // so an outage cannot fail a payment: it only delays the notification.
    PaymentResult result = pay("100.00", "during-outage");
    assertThat(result)
        .as("the payment succeeds with no broker")
        .isInstanceOf(PaymentResult.Completed.class);

    relay(unreachableBroker()).drain();

    OutboxRow afterFailure = row();
    assertThat(afterFailure.publishedAt()).as("not published").isNull();
    assertThat(afterFailure.parkedAt()).as("an outage is not a reason to park").isNull();
    assertThat(afterFailure.attempts()).isEqualTo(1);
    assertThat(afterFailure.lastError()).contains("TimeoutException");
    assertThat(afterFailure.nextAttemptAt()).as("first backoff").isEqualTo(START.plusSeconds(1));
    assertThat(gauge("payment.outbox.pending")).isEqualTo(1);

    // Kafka returns, the backoff elapses, and the backlog drains with no intervention.
    clock.advance(Duration.ofSeconds(2));
    relay(liveBroker()).drain();

    assertThat(consume(1)).hasSize(1);
    assertThat(row().publishedAt()).isNotNull();
    assertThat(gauge("payment.outbox.pending")).isZero();
  }

  @Test
  @DisplayName("a retriable failure is retried indefinitely and never parked")
  void neverParksForAnOutage() {
    // Well past MAX_ATTEMPTS. Were an outage counted toward parking, every event produced during a
    // long incident would end up parked and need replaying by hand.
    pay("10.00", "long-outage");
    OutboxRelay relay = relay(unreachableBroker());

    for (int attempt = 0; attempt < MAX_ATTEMPTS + 3; attempt++) {
      relay.drain();
      clock.advance(Duration.ofMinutes(2)); // past any backoff
    }

    OutboxRow row = row();
    assertThat(row.attempts()).isEqualTo(MAX_ATTEMPTS + 3);
    assertThat(row.parkedAt()).isNull();
    assertThat(registry.find("payment.outbox.parked").counter().count()).isZero();
  }

  @Test
  @DisplayName("a non-retriable failure is parked after the attempt limit, and not retried after")
  void parksAPoisonMessage() {
    // RecordTooLargeException: no amount of retrying will make the record fit.
    pay("10.00", "poison");
    OutboxRelay relay = relay(brokerRejectingEveryRecord());

    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      relay.drain();
      clock.advance(Duration.ofMinutes(2));
    }

    OutboxRow row = row();
    assertThat(row.parkedAt()).as("parked after the limit").isNotNull();
    assertThat(row.attempts()).isEqualTo(MAX_ATTEMPTS);
    assertThat(row.lastError()).contains("RecordTooLargeException");
    assertThat(registry.find("payment.outbox.parked").counter().count()).isEqualTo(1);

    // Parked means left alone: it no longer counts as pending, and is not claimed again.
    assertThat(relay.drain()).isZero();
    assertThat(gauge("payment.outbox.pending")).isZero();
  }

  @Test
  @DisplayName("concurrent relays on separate replicas publish every event exactly once")
  void concurrentRelaysDoNotDuplicate() throws Exception {
    // Every replica runs a relay. SKIP LOCKED is what keeps them from publishing the same rows;
    // this is its end-to-end evidence, with no failures in play so any duplicate would be a defect.
    int events = 200;
    transactions.executeWithoutResult(
        status -> {
          for (int i = 0; i < events; i++) {
            outbox.enqueue(
                OutboxMessage.pending(
                    UUID.randomUUID(),
                    account.toString(),
                    PaymentEvents.EVENT_TYPE,
                    new byte[] {1},
                    START));
          }
        });

    OutboxRelay first = relay(liveBroker(), 20);
    OutboxRelay second = relay(liveBroker(), 20);
    CompletableFuture<Void> a = CompletableFuture.runAsync(() -> drainCompletely(first));
    CompletableFuture<Void> b = CompletableFuture.runAsync(() -> drainCompletely(second));
    CompletableFuture.allOf(a, b).get();

    List<ConsumerRecord<String, byte[]>> records = consume(events);
    Set<String> distinct = new HashSet<>();
    records.forEach(r -> distinct.add(header(r, OutboxRelay.HEADER_PAYMENT_ID)));

    assertThat(records).hasSize(events);
    assertThat(distinct).as("no event published twice").hasSize(events);
    assertThat(outbox.backlog().pending()).isZero();
  }

  // --- fixtures -----------------------------------------------------------------------------

  private PaymentResult pay(String amount, String key) {
    return service.submit(
        new PaymentRequest(
            owner,
            account,
            new IdempotencyKey(key),
            Money.of(amount, "CHF"),
            new Beneficiary("Acme GmbH", "CH9300762011623852957"),
            null));
  }

  private OutboxRelay relay(KafkaTemplate<String, byte[]> kafka) {
    return relay(kafka, 100);
  }

  private OutboxRelay relay(KafkaTemplate<String, byte[]> kafka, int batchSize) {
    OutboxProperties properties =
        new OutboxProperties(
            true,
            topic,
            Duration.ofMillis(500),
            batchSize,
            MAX_ATTEMPTS,
            Duration.ofSeconds(1),
            Duration.ofMinutes(1),
            Duration.ofSeconds(10));
    return new OutboxRelay(outbox, kafka, transactions, metrics, properties, clock);
  }

  private static void drainCompletely(OutboxRelay relay) {
    while (relay.drain() > 0) {
      // keep claiming until this relay finds nothing left
    }
  }

  private KafkaTemplate<String, byte[]> liveBroker() {
    return template(KAFKA.getBootstrapServers(), Map.of());
  }

  /** Nothing listens on port 1: indistinguishable, to the client, from a broker that is down. */
  private KafkaTemplate<String, byte[]> unreachableBroker() {
    return template(
        "localhost:1",
        Map.of(
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 500,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000));
  }

  /** Every record exceeds the size limit, which Kafka reports as a non-retriable error. */
  private KafkaTemplate<String, byte[]> brokerRejectingEveryRecord() {
    return template(KAFKA.getBootstrapServers(), Map.of(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1));
  }

  private KafkaTemplate<String, byte[]> template(String bootstrap, Map<String, Object> overrides) {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.putAll(overrides);
    DefaultKafkaProducerFactory<String, byte[]> factory = new DefaultKafkaProducerFactory<>(config);
    factories.add(factory);
    return new KafkaTemplate<>(factory);
  }

  private List<ConsumerRecord<String, byte[]>> consume(int expected) {
    Map<String, Object> config =
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
            ByteArrayDeserializer.class);
    List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + 20_000;
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
      }
      // Linger briefly past the expected count, so a duplicate would be caught rather than left
      // unread.
      consumer.poll(Duration.ofSeconds(1)).forEach(records::add);
    }
    return records;
  }

  private static String header(ConsumerRecord<String, byte[]> record, String name) {
    return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
  }

  private double gauge(String name) {
    return registry.find(name).gauge().value();
  }

  private record OutboxRow(
      Instant publishedAt,
      Instant parkedAt,
      int attempts,
      String lastError,
      Instant nextAttemptAt) {}

  /** The single outbox row these tests create. */
  private OutboxRow row() {
    return jdbc.sql(
            "SELECT published_at, parked_at, attempts, last_error, next_attempt_at FROM outbox")
        .query(
            (rs, n) ->
                new OutboxRow(
                    instant(rs.getTimestamp("published_at")),
                    instant(rs.getTimestamp("parked_at")),
                    rs.getInt("attempts"),
                    rs.getString("last_error"),
                    instant(rs.getTimestamp("next_attempt_at"))))
        .single();
  }

  private static Instant instant(java.sql.Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
