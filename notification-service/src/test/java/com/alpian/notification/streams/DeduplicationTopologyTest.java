package com.alpian.notification.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.notification.config.NotificationProperties;
import com.alpian.notification.observability.NotificationMetrics;
import com.alpian.notification.support.Events;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The topology, driven synchronously by {@link TopologyTestDriver} with a mocked wall clock: every
 * record is fully processed when {@code pipeInput} returns, and the purge runs exactly when the
 * test advances time.
 */
class DeduplicationTopologyTest {

  private static final Instant START = Instant.parse("2026-10-08T09:00:00Z");
  private static final Duration RETENTION = Duration.ofDays(8);
  private static final Duration PURGE_INTERVAL = Duration.ofHours(1);
  private static final String ACCOUNT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";

  @TempDir Path stateDir;

  private SimpleMeterRegistry registry;
  private TopologyTestDriver driver;
  private TestInputTopic<String, byte[]> input;
  private TestOutputTopic<String, byte[]> notifications;
  private TestOutputTopic<String, byte[]> deadLetters;
  private KeyValueStore<String, Long> store;

  @BeforeEach
  void setUp() {
    NotificationProperties properties =
        new NotificationProperties(
            "payment-events",
            "notification-events",
            "payment-events-dlt",
            RETENTION,
            PURGE_INTERVAL,
            new NotificationProperties.Delivery(
                "notification-events-dlt", 5, Duration.ofSeconds(1), Duration.ofSeconds(30)));
    registry = new SimpleMeterRegistry();
    Topology topology =
        DeduplicationTopology.addTo(new Topology(), properties, new NotificationMetrics(registry));

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-service-test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    driver = new TopologyTestDriver(topology, config, START);

    input =
        driver.createInputTopic(
            "payment-events", Serdes.String().serializer(), Serdes.ByteArray().serializer());
    notifications =
        driver.createOutputTopic(
            "notification-events",
            Serdes.String().deserializer(),
            Serdes.ByteArray().deserializer());
    deadLetters =
        driver.createOutputTopic(
            "payment-events-dlt",
            Serdes.String().deserializer(),
            Serdes.ByteArray().deserializer());
    store = driver.getKeyValueStore(DeduplicationTopology.STORE);
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Nested
  class Deduplication {

    @Test
    @DisplayName("a payment event becomes one notification, keyed and headed for its account")
    void notifies() throws Exception {
      PaymentEvent event = Events.completed(ACCOUNT).build();

      pipe(event);

      TestRecord<String, byte[]> record = notifications.readRecord();
      NotificationEvent notification = NotificationEvent.parseFrom(record.value());
      assertThat(record.key()).isEqualTo(ACCOUNT);
      assertThat(notification.getPaymentId()).isEqualTo(event.getPaymentId());
      assertThat(notification.getMessage())
          .isEqualTo("Your payment of CHF 250.50 to Acme GmbH (Invoice 42) has been sent.");
      assertThat(notification.getNotifiedAt().getSeconds()).isEqualTo(START.getEpochSecond());
      assertThat(header(record.headers(), "payment-id")).isEqualTo(event.getPaymentId());
      assertThat(header(record.headers(), "content-type")).isEqualTo("application/x-protobuf");
      assertThat(header(record.headers(), "event-type"))
          .isEqualTo("alpian.payment.v1.NotificationEvent");
      assertThat(record.headers().headers("event-type")).hasSize(1);
      assertThat(notifications.isEmpty()).isTrue();
      assertThat(deadLetters.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a redelivered event is suppressed: one notification, however many copies")
    void suppressesDuplicates() {
      PaymentEvent event = Events.completed(ACCOUNT).build();

      pipe(event);
      pipe(event);
      pipe(event);

      assertThat(notifications.readValuesToList()).hasSize(1);
      assertThat(events("notified")).isEqualTo(1);
      assertThat(events("duplicate_suppressed")).isEqualTo(2);
    }

    @Test
    @DisplayName("a declined payment is notified too, and deduplicated the same way")
    void declined() throws Exception {
      PaymentEvent event = Events.declined(ACCOUNT).build();

      pipe(event);
      pipe(event);

      List<byte[]> values = notifications.readValuesToList();
      assertThat(values).hasSize(1);
      NotificationEvent notification = NotificationEvent.parseFrom(values.get(0));
      assertThat(notification.getStatus()).isEqualTo(PaymentStatus.PAYMENT_STATUS_FAILED);
      assertThat(notification.getMessage()).contains("declined: Insufficient funds");
    }

    @Test
    @DisplayName("distinct payments on the same account are each notified, in order")
    void distinctPayments() throws Exception {
      PaymentEvent first = Events.completed(ACCOUNT).build();
      PaymentEvent second = Events.completed(ACCOUNT).build();

      pipe(first);
      pipe(second);
      pipe(first);

      List<byte[]> values = notifications.readValuesToList();
      assertThat(values).hasSize(2);
      assertThat(NotificationEvent.parseFrom(values.get(0)).getPaymentId())
          .isEqualTo(first.getPaymentId());
      assertThat(NotificationEvent.parseFrom(values.get(1)).getPaymentId())
          .isEqualTo(second.getPaymentId());
    }

    @Test
    @DisplayName("the store records the time a payment was first notified")
    void recordsFirstSighting() {
      PaymentEvent event = Events.completed(ACCOUNT).build();

      pipe(event);
      driver.advanceWallClockTime(Duration.ofMinutes(5));
      pipe(event);

      assertThat(store.get(event.getPaymentId())).isEqualTo(START.toEpochMilli());
    }
  }

  @Nested
  class Retention {

    @Test
    @DisplayName("an id within the retention is kept, so a late duplicate is still suppressed")
    void keptWithinRetention() {
      PaymentEvent event = Events.completed(ACCOUNT).build();
      pipe(event);

      driver.advanceWallClockTime(RETENTION.minusHours(1));
      pipe(event);

      assertThat(notifications.readValuesToList()).hasSize(1);
      assertThat(store.get(event.getPaymentId())).isNotNull();
      assertThat(purged()).isZero();
    }

    @Test
    @DisplayName("an id past the retention is purged, keeping the store bounded")
    void purgedAfterRetention() {
      PaymentEvent expired = Events.completed(ACCOUNT).build();
      pipe(expired);
      driver.advanceWallClockTime(Duration.ofDays(2));
      PaymentEvent recent = Events.completed(ACCOUNT).build();
      pipe(recent);

      driver.advanceWallClockTime(RETENTION.minusDays(2).plus(PURGE_INTERVAL));

      assertThat(store.get(expired.getPaymentId())).isNull();
      assertThat(store.get(recent.getPaymentId())).isNotNull();
      assertThat(purged()).isEqualTo(1);
    }

    @Test
    @DisplayName("past the retention a redelivery is notified again, hence retention > topic's")
    void redeliveryAfterPurgeIsNotifiedAgain() {
      // Documents the boundary rather than desirable behaviour: it is why the retention (8 days)
      // must exceed the input topic's (7 days), so no copy of an event can outlive its id.
      PaymentEvent event = Events.completed(ACCOUNT).build();
      pipe(event);

      driver.advanceWallClockTime(RETENTION.plus(PURGE_INTERVAL));
      pipe(event);

      assertThat(notifications.readValuesToList()).hasSize(2);
    }
  }

  @Nested
  class DeadLettering {

    @Test
    @DisplayName("an undecodable value is dead-lettered with its original bytes and headers")
    void undecodable() {
      byte[] garbage = "not protobuf".getBytes(StandardCharsets.UTF_8);
      Headers headers = new RecordHeaders().add("payment-id", utf8("abc"));

      input.pipeInput(new TestRecord<>(ACCOUNT, garbage, headers, START));

      TestRecord<String, byte[]> record = deadLetters.readRecord();
      assertThat(record.key()).isEqualTo(ACCOUNT);
      assertThat(record.value()).isEqualTo(garbage);
      assertThat(header(record.headers(), "payment-id")).isEqualTo("abc");
      assertThat(header(record.headers(), DeadLetters.REASON)).isEqualTo("undecodable");
      assertThat(header(record.headers(), DeadLetters.ERROR)).isNotBlank();
      assertThat(header(record.headers(), DeadLetters.ORIGINAL_TOPIC)).isEqualTo("payment-events");
      assertThat(header(record.headers(), DeadLetters.ORIGINAL_PARTITION)).isEqualTo("0");
      assertThat(header(record.headers(), DeadLetters.ORIGINAL_OFFSET)).isEqualTo("0");
      assertThat(notifications.isEmpty()).isTrue();
      assertThat(events("dead_lettered")).isEqualTo(1);
    }

    @Test
    @DisplayName("a record with no value is dead-lettered as undecodable")
    void tombstone() {
      input.pipeInput(ACCOUNT, (byte[]) null);

      assertThat(header(deadLetters.readRecord().headers(), DeadLetters.REASON))
          .isEqualTo("undecodable");
    }

    @Test
    @DisplayName("an event missing its payment id is dead-lettered as invalid")
    void invalid() {
      input.pipeInput(ACCOUNT, Events.completed(ACCOUNT).clearPaymentId().build().toByteArray());

      TestRecord<String, byte[]> record = deadLetters.readRecord();
      assertThat(header(record.headers(), DeadLetters.REASON)).isEqualTo("invalid_event");
      assertThat(header(record.headers(), DeadLetters.ERROR)).contains("payment_id");
    }

    @Test
    @DisplayName("an event keyed by anything but its account id is dead-lettered, not notified")
    void keyMismatch() {
      PaymentEvent event = Events.completed(ACCOUNT).build();

      input.pipeInput(event.getPaymentId(), event.toByteArray());

      assertThat(header(deadLetters.readRecord().headers(), DeadLetters.REASON))
          .isEqualTo("key_mismatch");
      assertThat(store.get(event.getPaymentId())).isNull();
      assertThat(notifications.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a dead-lettered record does not block the partition behind it")
    void doesNotBlock() {
      input.pipeInput(ACCOUNT, "garbage".getBytes(StandardCharsets.UTF_8));
      pipe(Events.completed(ACCOUNT).build());

      assertThat(deadLetters.readValuesToList()).hasSize(1);
      assertThat(notifications.readValuesToList()).hasSize(1);
    }

    @Test
    @DisplayName("a reason header already on the record is replaced, not duplicated")
    void replacesReasonHeader() {
      Headers headers = new RecordHeaders().add(DeadLetters.REASON, utf8("stale"));

      input.pipeInput(new TestRecord<>(ACCOUNT, utf8("garbage"), headers, START));

      Headers out = deadLetters.readRecord().headers();
      assertThat(out.headers(DeadLetters.REASON)).hasSize(1);
      assertThat(header(out, DeadLetters.REASON)).isEqualTo("undecodable");
    }
  }

  private void pipe(PaymentEvent event) {
    Headers headers =
        new RecordHeaders()
            .add("payment-id", utf8(event.getPaymentId()))
            .add("event-type", utf8("alpian.payment.v1.PaymentEvent"))
            .add("content-type", utf8("application/x-protobuf"));
    input.pipeInput(
        new TestRecord<>(event.getAccountId(), event.toByteArray(), headers, (Instant) null));
  }

  private double events(String outcome) {
    return registry.get("notification.events").tag("outcome", outcome).counter().count();
  }

  private double purged() {
    return registry.get("notification.deduplication.purged").counter().count();
  }

  private static String header(Headers headers, String key) {
    var header = headers.lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
