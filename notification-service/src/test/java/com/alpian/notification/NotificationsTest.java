package com.alpian.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.PaymentStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NotificationsTest {

  @ParameterizedTest(name = "{0} {1} reads \"{2}\"")
  @CsvSource({
    "250.5000, CHF, CHF 250.50",
    "1000.0000, JPY, JPY 1000",
    "12.3450, CHF, CHF 12.345",
  })
  void formatsAtCurrencyPrecisionWithoutRounding(String amount, String currency, String expected) {
    Money money = Money.newBuilder().setAmount(amount).setCurrency(currency).build();

    assertThat(Notifications.format(money)).isEqualTo(expected);
  }

  @Test
  void describesACompletedPayment() {
    assertThat(Notifications.message(Events.completed("a").build()))
        .isEqualTo("Your payment of CHF 250.50 to Acme GmbH (Invoice 42) has been sent.");
  }

  @Test
  void describesADeclinedPayment() {
    var declined =
        Events.completed("a")
            .clearReference()
            .setStatus(PaymentStatus.PAYMENT_STATUS_FAILED)
            .setFailureReason("Insufficient funds")
            .build();

    assertThat(Notifications.message(declined))
        .isEqualTo("Your payment of CHF 250.50 to Acme GmbH was declined: Insufficient funds.");
  }
}
