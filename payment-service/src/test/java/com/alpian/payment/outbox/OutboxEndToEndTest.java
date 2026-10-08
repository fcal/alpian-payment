package com.alpian.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.repository.AccountRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * HTTP request to Kafka record, through the application as Spring assembles it: the real
 * controller, the transactional executor, and the relay running on its own schedule.
 *
 * <p>The relay suite drives {@code drain()} by hand for determinism, which leaves one thing
 * unproven — that the scheduled relay is actually wired and running. That is what this checks.
 */
@SpringBootTest(
    properties = {
      "spring.docker.compose.enabled=false",
      "payment.outbox.relay-enabled=true",
      "payment.outbox.poll-interval=100ms"
    })
@AutoConfigureMockMvc
class OutboxEndToEndTest {

  @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  static final String TOPIC = "payment-events-e2e";

  static {
    POSTGRES.start();
    KAFKA.start();
    try (AdminClient admin =
        AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
    } catch (Exception e) {
      throw new IllegalStateException("Could not create test topic", e);
    }
  }

  @DynamicPropertySource
  static void kafka(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("payment.outbox.topic", () -> TOPIC);
  }

  @Autowired MockMvc mvc;
  @Autowired AccountRepository accounts;
  @Autowired JdbcClient jdbc;
  @Autowired ObjectMapper json;

  @Test
  @DisplayName("a payment submitted over HTTP reaches Kafka with no manual step")
  void paymentReachesKafka() throws Exception {
    UserId user = new UserId(UUID.randomUUID());
    AccountId account = new AccountId(UUID.randomUUID());
    jdbc.sql("INSERT INTO app_user (id, name) VALUES (:id, 'E2E')")
        .param("id", user.value())
        .update();
    accounts.save(
        new Account(account, user, Money.of("500.00", "CHF"), Instant.now(), Instant.now()));

    String response =
        mvc.perform(
                post("/api/v1/users/{u}/accounts/{a}/payments", user.value(), account.value())
                    .header("Idempotency-Key", "e2e")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"amount":{"value":"42.00","currency":"CHF"},
                         "beneficiary":{"name":"Acme GmbH","iban":"CH9300762011623852957"}}
                        """))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();
    String paymentId = json.readTree(response).get("paymentId").asText();

    ConsumerRecord<String, byte[]> record = awaitRecordFor(paymentId);

    PaymentEvent event = PaymentEvent.parseFrom(record.value());
    assertThat(record.key()).isEqualTo(account.toString());
    assertThat(event.getAmount().getAmount()).isEqualTo("42.0000");
    assertThat(event.getUserId()).isEqualTo(user.toString());
  }

  private ConsumerRecord<String, byte[]> awaitRecordFor(String paymentId) {
    Map<String, Object> config =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "e2e-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class);
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(List.of(TOPIC));
      long deadline = System.currentTimeMillis() + 20_000;
      List<ConsumerRecord<String, byte[]>> seen = new ArrayList<>();
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
          seen.add(record);
          if (paymentId.equals(header(record))) {
            return record;
          }
        }
      }
      throw new AssertionError(
          "No event for payment " + paymentId + " within 20s; saw " + seen.size() + " records");
    }
  }

  private static String header(ConsumerRecord<String, byte[]> record) {
    var header = record.headers().lastHeader(OutboxRelay.HEADER_PAYMENT_ID);
    return header == null
        ? null
        : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
  }
}
