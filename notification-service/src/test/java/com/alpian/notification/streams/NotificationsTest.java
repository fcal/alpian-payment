package com.alpian.notification.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.notification.support.Events;
import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import java.time.Instant;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NotificationsTest {

  private static final String ACCOUNT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";

  @Nested
  class Formatting {

    @ParameterizedTest(name = "{0} {1} reads \"{2}\"")
    @CsvSource({
      "250.5000, CHF, CHF 250.50",
      "1000.0000, CHF, CHF 1000.00",
      "0.0500, EUR, EUR 0.05",
      "1000.0000, JPY, JPY 1000",
      "12.3450, CHF, CHF 12.345",
    })
    @DisplayName("amounts are shown at the currency's precision, and never rounded")
    void formatsAtCurrencyPrecision(String amount, String currency, String expected) {
      Money money = Money.newBuilder().setAmount(amount).setCurrency(currency).build();

      assertThat(Notifications.format(money)).isEqualTo(expected);
    }

    @Test
    @DisplayName("a completed payment reads as sent, with its reference")
    void completedMessage() {
      assertThat(Notifications.message(Events.completed(ACCOUNT).build()))
          .isEqualTo("Your payment of CHF 250.50 to Acme GmbH (Invoice 42) has been sent.");
    }

    @Test
    @DisplayName("a declined payment reads as declined, with the reason")
    void declinedMessage() {
      assertThat(Notifications.message(Events.declined(ACCOUNT).clearReference().build()))
          .isEqualTo("Your payment of CHF 2000.00 to Acme GmbH was declined: Insufficient funds.");
    }

    @Test
    @DisplayName("the notification carries the payment's identity, amount and status")
    void notificationFields() {
      PaymentEvent event = Events.completed(ACCOUNT).build();
      Instant at = Instant.parse("2026-10-08T09:00:00.123Z");

      NotificationEvent notification = Notifications.from(event, at);

      assertThat(notification.getPaymentId()).isEqualTo(event.getPaymentId());
      assertThat(notification.getUserId()).isEqualTo(event.getUserId());
      assertThat(notification.getAccountId()).isEqualTo(ACCOUNT);
      assertThat(notification.getAmount()).isEqualTo(event.getAmount());
      assertThat(notification.getStatus()).isEqualTo(PaymentStatus.PAYMENT_STATUS_COMPLETED);
      assertThat(notification.getNotifiedAt().getSeconds()).isEqualTo(at.getEpochSecond());
      assertThat(notification.getNotifiedAt().getNanos()).isEqualTo(123_000_000);
    }
  }

  @Nested
  class Validation {

    @Test
    @DisplayName("a well-formed event under its account-id key is accepted")
    void accepts() {
      assertThat(Notifications.validate(Events.completed(ACCOUNT).build(), ACCOUNT)).isEmpty();
      assertThat(Notifications.validate(Events.declined(ACCOUNT).build(), ACCOUNT)).isEmpty();
    }

    @Test
    @DisplayName("an event keyed by anything but its account id is a key mismatch")
    void keyMismatch() {
      PaymentEvent event = Events.completed(ACCOUNT).build();

      assertThat(Notifications.validate(event, "another-key"))
          .hasValueSatisfying(r -> assertThat(r.reason()).isEqualTo(DeadLetterReason.KEY_MISMATCH));
      assertThat(Notifications.validate(event, null))
          .hasValueSatisfying(r -> assertThat(r.reason()).isEqualTo(DeadLetterReason.KEY_MISMATCH));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
      "missing payment id",
      "malformed payment id",
      "missing account id",
      "missing user id",
      "missing amount",
      "malformed amount",
      "non-positive amount",
      "unknown currency",
      "unspecified status",
      "unrecognised status",
    })
    @DisplayName("an event that cannot be notified is invalid")
    void invalid(String defect) {
      PaymentEvent event = defective(defect).apply(Events.completed(ACCOUNT)).build();

      assertThat(Notifications.validate(event, ACCOUNT))
          .hasValueSatisfying(
              r -> assertThat(r.reason()).isEqualTo(DeadLetterReason.INVALID_EVENT));
    }

    private UnaryOperator<PaymentEvent.Builder> defective(String defect) {
      return switch (defect) {
        case "missing payment id" -> b -> b.clearPaymentId();
        case "malformed payment id" -> b -> b.setPaymentId("not-a-uuid");
        case "missing account id" -> b -> b.clearAccountId();
        case "missing user id" -> b -> b.clearUserId();
        case "missing amount" -> b -> b.clearAmount();
        case "malformed amount" -> b -> b.setAmount(b.getAmount().toBuilder().setAmount("1,00"));
        case "non-positive amount" -> b -> b.setAmount(b.getAmount().toBuilder().setAmount("0"));
        case "unknown currency" -> b -> b.setAmount(b.getAmount().toBuilder().setCurrency("XYZ"));
        case "unspecified status" -> b -> b.setStatus(PaymentStatus.PAYMENT_STATUS_UNSPECIFIED);
        case "unrecognised status" -> b -> b.setStatusValue(99);
        default -> throw new IllegalArgumentException(defect);
      };
    }
  }
}
