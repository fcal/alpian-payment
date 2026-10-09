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

  /**
   * Why {@code event} cannot be notified, or null if it can. Checked before notifying, so a bad
   * event is skipped instead of throwing inside the topology and stopping every stream thread.
   */
  static String problem(PaymentEvent event) {
    if (event.getUserId().isEmpty()) {
      return "user_id is missing";
    }
    if (!event.hasAmount()) {
      return "amount is missing";
    }
    try {
      if (new BigDecimal(event.getAmount().getAmount()).signum() <= 0) {
        return "amount is not positive";
      }
      Currency.getInstance(event.getAmount().getCurrency());
    } catch (IllegalArgumentException e) { // also NumberFormatException
      return "amount is malformed";
    }
    // Includes UNRECOGNIZED, a status from a newer producer: never notify with a guessed message.
    if (event.getStatus() != PaymentStatus.PAYMENT_STATUS_COMPLETED
        && event.getStatus() != PaymentStatus.PAYMENT_STATUS_FAILED) {
      return "unsupported status " + event.getStatus();
    }
    return null;
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
