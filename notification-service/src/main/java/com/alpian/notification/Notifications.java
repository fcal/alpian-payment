package com.alpian.notification;

import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import com.google.protobuf.Timestamp;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;

/** Turns a payment event into the notification sent to the payer. */
final class Notifications {

  private Notifications() {}

  static NotificationEvent from(PaymentEvent event, Instant notifiedAt) {
    return NotificationEvent.newBuilder()
        .setPaymentId(event.getPaymentId())
        .setUserId(event.getUserId())
        .setAccountId(event.getAccountId())
        .setAmount(event.getAmount())
        .setStatus(event.getStatus())
        .setMessage(message(event))
        .setNotifiedAt(
            Timestamp.newBuilder()
                .setSeconds(notifiedAt.getEpochSecond())
                .setNanos(notifiedAt.getNano()))
        .build();
  }

  static String message(PaymentEvent event) {
    String payment =
        "Your payment of "
            + format(event.getAmount())
            + " to "
            + event.getBeneficiaryName()
            + (event.getReference().isEmpty() ? "" : " (" + event.getReference() + ")");
    return event.getStatus() == PaymentStatus.PAYMENT_STATUS_COMPLETED
        ? payment + " has been sent."
        : payment + " was declined: " + event.getFailureReason() + ".";
  }

  /** {@code 250.5000 CHF} reads {@code CHF 250.50}; extra significant decimals are kept. */
  static String format(Money money) {
    BigDecimal amount = new BigDecimal(money.getAmount()).stripTrailingZeros();
    int digits = Math.max(Currency.getInstance(money.getCurrency()).getDefaultFractionDigits(), 0);
    return money.getCurrency()
        + " "
        + amount.setScale(Math.max(digits, amount.scale())).toPlainString();
  }
}
