package com.alpian.payment.outbox;

import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.repository.OutboxMessage;
import com.google.protobuf.Timestamp;

/**
 * Converts a journalled payment into its {@code PaymentEvent} and the outbox message carrying it.
 *
 * <p>Serialised at enqueue time, inside the payment transaction, rather than by the relay. The
 * event then records the payment exactly as it was committed, and the relay stays a dumb pipe that
 * moves bytes — it has no dependency on the domain and nothing to get wrong about it.
 */
public final class PaymentEvents {

  /** The protobuf message's full name, so a consumer can tell what the payload is. */
  public static final String EVENT_TYPE = PaymentEvent.getDescriptor().getFullName();

  private PaymentEvents() {}

  public static OutboxMessage toOutboxMessage(Payment payment, UserId payer) {
    return OutboxMessage.pending(
        payment.id().value(),
        // Keyed by account, not by payment: an account's events share a partition and stay
        // ordered, and co-partition with the consumer's state. A per-payment key would scatter
        // them across partitions.
        payment.accountId().toString(),
        EVENT_TYPE,
        toEvent(payment, payer).toByteArray(),
        payment.createdAt());
  }

  static PaymentEvent toEvent(Payment payment, UserId payer) {
    PaymentEvent.Builder event =
        PaymentEvent.newBuilder()
            .setPaymentId(payment.id().toString())
            .setAccountId(payment.accountId().toString())
            .setUserId(payer.toString())
            .setAmount(
                com.alpian.payment.events.v1.Money.newBuilder()
                    // Plain decimal string: protobuf has no decimal type, and a double would not
                    // round-trip the NUMERIC(19,4) value exactly.
                    .setAmount(payment.amount().amount().toPlainString())
                    .setCurrency(payment.amount().currencyCode()))
            .setStatus(toStatus(payment.status()))
            .setBeneficiaryName(payment.beneficiary().name())
            .setOccurredAt(toTimestamp(payment.createdAt()));

    // proto3 strings cannot be null; absent optional fields are simply left unset.
    payment.failureReasonIfAny().ifPresent(event::setFailureReason);
    payment.referenceIfAny().ifPresent(event::setReference);
    return event.build();
  }

  private static com.alpian.payment.events.v1.PaymentStatus toStatus(PaymentStatus status) {
    return switch (status) {
      case COMPLETED -> com.alpian.payment.events.v1.PaymentStatus.PAYMENT_STATUS_COMPLETED;
      case FAILED -> com.alpian.payment.events.v1.PaymentStatus.PAYMENT_STATUS_FAILED;
    };
  }

  private static Timestamp toTimestamp(java.time.Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
