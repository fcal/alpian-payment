package com.alpian.payment.outbox;

import com.alpian.payment.domain.Payment;
import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import com.google.protobuf.Timestamp;
import java.util.UUID;

/** Maps a journalled payment to its protobuf event. */
public final class PaymentEvents {

  public static final String EVENT_TYPE = PaymentEvent.getDescriptor().getFullName();

  private PaymentEvents() {}

  public static PaymentEvent toEvent(Payment payment, UUID userId) {
    PaymentEvent.Builder event =
        PaymentEvent.newBuilder()
            .setPaymentId(payment.id().toString())
            .setAccountId(payment.accountId().toString())
            .setUserId(userId.toString())
            .setAmount(
                Money.newBuilder()
                    .setAmount(payment.amount().toPlainString())
                    .setCurrency(payment.currency()))
            .setStatus(
                payment.isCompleted()
                    ? PaymentStatus.PAYMENT_STATUS_COMPLETED
                    : PaymentStatus.PAYMENT_STATUS_FAILED)
            .setBeneficiaryName(payment.beneficiaryName())
            .setOccurredAt(
                Timestamp.newBuilder()
                    .setSeconds(payment.createdAt().getEpochSecond())
                    .setNanos(payment.createdAt().getNano()));
    // proto3 strings cannot be null: absent values are left unset.
    if (payment.failureReason() != null) {
      event.setFailureReason(payment.failureReason());
    }
    if (payment.reference() != null) {
      event.setReference(payment.reference());
    }
    return event.build();
  }
}
