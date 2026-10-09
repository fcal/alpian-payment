package com.alpian.notification.streams;

import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.NotificationEvent;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import com.google.protobuf.Timestamp;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;
import java.util.Optional;
import java.util.UUID;

/** Validates payment events and turns them into the notification sent to the payer. */
public final class Notifications {

  private Notifications() {}

  /** Why an event cannot be notified. */
  public record Rejection(DeadLetterReason reason, String detail) {}

  /**
   * Checks that {@code event}, received under {@code key}, can be turned into a notification.
   *
   * @return the reason it cannot, or empty if it can
   */
  public static Optional<Rejection> validate(PaymentEvent event, String key) {
    if (!isUuid(event.getPaymentId())) {
      return invalid("payment_id is missing or not a UUID: '" + event.getPaymentId() + "'");
    }
    if (event.getAccountId().isEmpty()) {
      return invalid("account_id is missing");
    }
    if (event.getUserId().isEmpty()) {
      return invalid("user_id is missing");
    }
    if (!event.hasAmount()) {
      return invalid("amount is missing");
    }
    try {
      if (new BigDecimal(event.getAmount().getAmount()).signum() <= 0) {
        return invalid("amount is not positive: " + event.getAmount().getAmount());
      }
      Currency.getInstance(event.getAmount().getCurrency());
    } catch (IllegalArgumentException e) {
      return invalid(
          "amount is malformed: '"
              + event.getAmount().getAmount()
              + " "
              + event.getAmount().getCurrency()
              + "'");
    }
    if (event.getStatus() != PaymentStatus.PAYMENT_STATUS_COMPLETED
        && event.getStatus() != PaymentStatus.PAYMENT_STATUS_FAILED) {
      // Includes UNRECOGNIZED: a status added by a newer producer is parked for inspection rather
      // than notified with a guessed message.
      return invalid("unsupported status " + event.getStatus());
    }
    if (!event.getAccountId().equals(key)) {
      return Optional.of(
          new Rejection(
              DeadLetterReason.KEY_MISMATCH,
              "record key '" + key + "' is not the account id '" + event.getAccountId() + "'"));
    }
    return Optional.empty();
  }

  /** The notification for a validated {@code event}. */
  public static NotificationEvent from(PaymentEvent event, Instant notifiedAt) {
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
        "Your payment of " + format(event.getAmount()) + " to " + event.getBeneficiaryName();
    String reference = event.getReference().isEmpty() ? "" : " (" + event.getReference() + ")";
    return switch (event.getStatus()) {
      case PAYMENT_STATUS_COMPLETED -> payment + reference + " has been sent.";
      case PAYMENT_STATUS_FAILED ->
          payment + reference + " was declined: " + event.getFailureReason() + ".";
      default -> throw new IllegalArgumentException("unsupported status " + event.getStatus());
    };
  }

  /**
   * Formats an amount at its currency's precision, so {@code 250.5000 CHF} reads {@code CHF 250.50}
   * and {@code 1000.0000 JPY} reads {@code JPY 1000}. An amount carrying more significant decimals
   * than the currency has is shown in full rather than rounded: a notification must never state a
   * different amount from the one paid.
   */
  static String format(Money money) {
    Currency currency = Currency.getInstance(money.getCurrency());
    BigDecimal amount = new BigDecimal(money.getAmount());
    int digits = Math.max(currency.getDefaultFractionDigits(), 0);
    BigDecimal shown =
        amount.stripTrailingZeros().scale() > digits
            ? amount.stripTrailingZeros()
            : amount.setScale(digits);
    return currency.getCurrencyCode() + " " + shown.toPlainString();
  }

  private static boolean isUuid(String value) {
    try {
      UUID.fromString(value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static Optional<Rejection> invalid(String detail) {
    return Optional.of(new Rejection(DeadLetterReason.INVALID_EVENT, detail));
  }
}
