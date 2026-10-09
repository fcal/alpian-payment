package com.alpian.notification.support;

import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import com.google.protobuf.Timestamp;
import java.util.UUID;

/** Builders for payment events as the payment service's outbox relay publishes them. */
public final class Events {

  public static final String USER = "11111111-1111-1111-1111-111111111111";

  private Events() {}

  /** A completed payment of CHF 250.50 from {@code accountId}. */
  public static PaymentEvent.Builder completed(String accountId) {
    return PaymentEvent.newBuilder()
        .setPaymentId(UUID.randomUUID().toString())
        .setAccountId(accountId)
        .setUserId(USER)
        .setAmount(Money.newBuilder().setAmount("250.5000").setCurrency("CHF"))
        .setStatus(PaymentStatus.PAYMENT_STATUS_COMPLETED)
        .setBeneficiaryName("Acme GmbH")
        .setReference("Invoice 42")
        .setOccurredAt(Timestamp.newBuilder().setSeconds(1_791_450_000));
  }

  /** A payment from {@code accountId} declined for insufficient funds. */
  public static PaymentEvent.Builder declined(String accountId) {
    return completed(accountId)
        .setAmount(Money.newBuilder().setAmount("2000.0000").setCurrency("CHF"))
        .setStatus(PaymentStatus.PAYMENT_STATUS_FAILED)
        .setFailureReason("Insufficient funds");
  }
}
