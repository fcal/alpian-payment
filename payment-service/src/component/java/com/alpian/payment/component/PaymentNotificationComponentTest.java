package com.alpian.payment.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.alpian.payment.component.PaymentApi.Response;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End to end, through every hop a payment takes: HTTP request, PostgreSQL journal, outbox, Kafka,
 * deduplication, delivery.
 *
 * <p>Delivery is observed in the notification service's own log, where its sender writes each
 * message: that line is the side effect a real provider call would replace, so it is the last hop
 * there is to assert on.
 *
 * <p>Each test creates its own user and account, so tests share the stack without depending on one
 * another or on the seeded demo data.
 */
class PaymentNotificationComponentTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private final PaymentApi api = new PaymentApi(ComponentStack.paymentServiceUrl());

  private UUID user;
  private UUID account;

  @BeforeEach
  void givenAccount() {
    user = UUID.randomUUID();
    account = UUID.randomUUID();
    sql("INSERT INTO app_user (id, name) VALUES (?, 'Component Test')", user);
    sql(
        "INSERT INTO account (id, user_id, balance, currency) VALUES (?, ?, 1000.00, 'CHF')",
        account,
        user);
  }

  @Test
  @DisplayName("a payment travels from the API through Postgres and Kafka to a notification")
  void paymentReachesThePayer() throws Exception {
    Response created = api.pay(user, account, "key-1", "120.00", "CHF");

    assertThat(created.status()).isEqualTo(201);
    assertThat(created.body().get("status").asText()).isEqualTo("COMPLETED");
    String paymentId = created.paymentId();

    // Journal and balance, committed together.
    assertThat(api.balance(user, account).body().at("/balance/value").asText()).isEqualTo("880.00");
    assertThat(queryString("SELECT status FROM payment WHERE id = ?", UUID.fromString(paymentId)))
        .contains("COMPLETED");

    // Outbox to Kafka: published, keyed by account, with the payment id as a header.
    ConsumerRecord<String, byte[]> published = awaitRecord("payment-events", paymentId);
    PaymentEvent event = PaymentEvent.parseFrom(published.value());
    assertThat(published.key()).isEqualTo(account.toString());
    assertThat(event.getAmount().getAmount()).isEqualTo("120.0000");
    assertThat(event.getStatus()).isEqualTo(PaymentStatus.PAYMENT_STATUS_COMPLETED);
    await()
        .atMost(TIMEOUT)
        .until(
            () ->
                queryString(
                        "SELECT published_at::text FROM outbox WHERE aggregate_id = ?",
                        UUID.fromString(paymentId))
                    .isPresent());

    // Deduplicated notification, then delivery.
    NotificationEvent notification =
        NotificationEvent.parseFrom(awaitRecord("notification-events", paymentId).value());
    assertThat(notification.getUserId()).isEqualTo(user.toString());
    assertThat(awaitDelivery(paymentId))
        .isEqualTo("Your payment of CHF 120.00 to Acme GmbH (Component test) has been sent.");
  }

  @Test
  @DisplayName("a retried request is debited once and notified once")
  void retriedRequestIsNotifiedOnce() {
    Response first = api.pay(user, account, "retry-key", "75.00", "CHF");
    Response retry = api.pay(user, account, "retry-key", "75.00", "CHF");

    assertThat(first.status()).isEqualTo(201);
    assertThat(retry.status()).isEqualTo(201);
    assertThat(retry.replayed()).isTrue();
    assertThat(retry.paymentId()).isEqualTo(first.paymentId());
    assertThat(api.balance(user, account).body().at("/balance/value").asText()).isEqualTo("925.00");

    // A later payment on the same account is delivered after anything the first could have
    // caused, so once it arrives, a second notification for the first would have too.
    String marker = api.pay(user, account, "marker", "1.00", "CHF").paymentId();
    awaitDelivery(marker);
    assertThat(deliveries(first.paymentId())).hasSize(1);
    assertThat(queryLong("SELECT count(*) FROM outbox WHERE partition_key = ?", account.toString()))
        .isEqualTo(2);
  }

  @Test
  @DisplayName("a declined payment is rejected with 409 and the payer is told why")
  void declinedPaymentIsNotified() {
    Response declined = api.pay(user, account, "too-much", "5000.00", "CHF");

    assertThat(declined.status()).isEqualTo(409);
    assertThat(declined.body().get("code").asText()).isEqualTo("insufficient_funds");
    assertThat(api.balance(user, account).body().at("/balance/value").asText())
        .isEqualTo("1000.00");
    assertThat(awaitDelivery(declined.paymentId()))
        .isEqualTo(
            "Your payment of CHF 5000.00 to Acme GmbH (Component test) was declined:"
                + " Insufficient funds.");
  }

  @Test
  @DisplayName("concurrent payments drain the account exactly, and every attempt is notified")
  void concurrentPaymentsNeverOverdraw() throws Exception {
    int requests = 20;
    List<Callable<Response>> payments = new ArrayList<>();
    for (int i = 0; i < requests; i++) {
      String key = "concurrent-" + i;
      payments.add(() -> api.pay(user, account, key, "100.00", "CHF"));
    }

    List<Response> responses = new ArrayList<>();
    ExecutorService pool = Executors.newFixedThreadPool(requests);
    try {
      for (Future<Response> response : pool.invokeAll(payments)) {
        responses.add(response.get());
      }
    } finally {
      pool.shutdown();
    }

    assertThat(responses).filteredOn(r -> r.status() == 201).hasSize(10);
    assertThat(responses).filteredOn(r -> r.status() == 409).hasSize(10);
    assertThat(api.balance(user, account).body().at("/balance/value").asText()).isEqualTo("0.00");
    for (Response response : responses) {
      awaitDelivery(response.paymentId());
    }
  }

  @Test
  @DisplayName("with Kafka unreachable, payments still succeed and are notified on recovery")
  void paymentsSurviveKafkaOutage() {
    Response created;
    ComponentStack.pauseKafka();
    try {
      long start = System.nanoTime();
      created = api.pay(user, account, "during-outage", "42.00", "CHF");
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      // The payment path never waits on Kafka.
      assertThat(created.status()).isEqualTo(201);
      assertThat(took).isLessThan(Duration.ofSeconds(2));
      assertThat(new BigDecimal(api.balance(user, account).body().at("/balance/value").asText()))
          .isEqualByComparingTo("958.00");

      // The relay is trying, failing, and keeping the event: retried, not parked.
      String paymentId = created.paymentId();
      await()
          .atMost(TIMEOUT)
          .until(
              () ->
                  queryLong(
                          "SELECT attempts FROM outbox WHERE aggregate_id = ?",
                          UUID.fromString(paymentId))
                      > 0);
      assertThat(
              queryString(
                  "SELECT coalesce(published_at, parked_at)::text FROM outbox WHERE aggregate_id = ?",
                  UUID.fromString(paymentId)))
          .isEmpty();
    } finally {
      ComponentStack.resumeKafka();
    }

    assertThat(awaitDelivery(created.paymentId())).contains("CHF 42.00");
  }

  // --- Delivery, as logged by the notification service's sender ---

  private static final Pattern DELIVERY =
      Pattern.compile("Notifying user \\S+ of payment (\\S+): (.*)");

  private String awaitDelivery(String paymentId) {
    return await()
        .atMost(TIMEOUT)
        .until(() -> deliveries(paymentId), list -> !list.isEmpty())
        .get(0);
  }

  private List<String> deliveries(String paymentId) {
    List<String> messages = new ArrayList<>();
    Matcher matcher = DELIVERY.matcher(ComponentStack.NOTIFICATION_SERVICE.getLogs());
    while (matcher.find()) {
      if (matcher.group(1).equals(paymentId)) {
        messages.add(matcher.group(2).strip());
      }
    }
    return messages;
  }

  // --- Kafka ---

  private ConsumerRecord<String, byte[]> awaitRecord(String topic, String paymentId) {
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ComponentStack.KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "component-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(250))) {
          var header = record.headers().lastHeader("payment-id");
          if (header != null
              && paymentId.equals(new String(header.value(), StandardCharsets.UTF_8))) {
            return record;
          }
        }
      }
    }
    throw new AssertionError("No record for payment " + paymentId + " on " + topic);
  }

  // --- SQL ---

  private static Connection connection() throws SQLException {
    return DriverManager.getConnection(
        ComponentStack.POSTGRES.getJdbcUrl(),
        ComponentStack.POSTGRES.getUsername(),
        ComponentStack.POSTGRES.getPassword());
  }

  private static void sql(String statement, Object... params) {
    try (Connection connection = connection();
        PreparedStatement prepared = prepare(connection, statement, params)) {
      prepared.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Optional<String> queryString(String query, Object... params) {
    try (Connection connection = connection();
        PreparedStatement prepared = prepare(connection, query, params);
        ResultSet result = prepared.executeQuery()) {
      return result.next() ? Optional.ofNullable(result.getString(1)) : Optional.empty();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static long queryLong(String query, Object... params) {
    try (Connection connection = connection();
        PreparedStatement prepared = prepare(connection, query, params);
        ResultSet result = prepared.executeQuery()) {
      result.next();
      return result.getLong(1);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static PreparedStatement prepare(
      Connection connection, String statement, Object... params) throws SQLException {
    PreparedStatement prepared = connection.prepareStatement(statement);
    for (int i = 0; i < params.length; i++) {
      prepared.setObject(i + 1, params[i]);
    }
    return prepared;
  }
}
