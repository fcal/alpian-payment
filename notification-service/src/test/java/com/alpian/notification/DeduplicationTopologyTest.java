package com.alpian.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeduplicationTopologyTest {

  private static final String ACCOUNT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";

  @TempDir Path stateDir;

  private SimpleMeterRegistry metrics;
  private TopologyTestDriver driver;
  private TestInputTopic<String, byte[]> input;
  private TestOutputTopic<String, byte[]> output;

  @BeforeEach
  void setUp() {
    metrics = new SimpleMeterRegistry();
    StreamsBuilder builder = new StreamsBuilder();
    DeduplicationTopology.build(builder, metrics);

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    driver = new TopologyTestDriver(builder.build(), config);
    input =
        driver.createInputTopic(
            DeduplicationTopology.INPUT_TOPIC,
            Serdes.String().serializer(),
            Serdes.ByteArray().serializer());
    output =
        driver.createOutputTopic(
            DeduplicationTopology.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.ByteArray().deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("a payment event becomes one notification, keyed by its account")
  void notifies() throws Exception {
    PaymentEvent event = Events.completed(ACCOUNT).build();

    pipe(event);

    TestRecord<String, byte[]> record = output.readRecord();
    NotificationEvent notification = NotificationEvent.parseFrom(record.value());
    assertThat(record.key()).isEqualTo(ACCOUNT);
    assertThat(notification.getPaymentId()).isEqualTo(event.getPaymentId());
    assertThat(notification.getMessage()).contains("CHF 250.50");
    assertThat(
            new String(record.headers().lastHeader("event-type").value(), StandardCharsets.UTF_8))
        .isEqualTo("alpian.payment.v1.NotificationEvent");
  }

  @Test
  @DisplayName("a redelivered event is suppressed: one notification however many copies")
  void suppressesDuplicates() {
    PaymentEvent event = Events.completed(ACCOUNT).build();
    PaymentEvent other = Events.completed(ACCOUNT).build();

    pipe(event);
    pipe(event);
    pipe(other);
    pipe(event);

    assertThat(output.readValuesToList()).hasSize(2);
    assertThat(metrics.counter("notification.events", "outcome", "duplicate").count()).isEqualTo(2);
  }

  @Test
  @DisplayName("an undecodable event is skipped and does not block the partition")
  void skipsUndecodableEvents() {
    input.pipeInput(ACCOUNT, "garbage".getBytes(StandardCharsets.UTF_8));
    input.pipeInput(ACCOUNT, (byte[]) null);
    pipe(Events.completed(ACCOUNT).build());

    assertThat(output.readValuesToList()).hasSize(1);
    assertThat(metrics.counter("notification.events", "outcome", "skipped").count()).isEqualTo(2);
  }

  private void pipe(PaymentEvent event) {
    input.pipeInput(event.getAccountId(), event.toByteArray());
  }
}
