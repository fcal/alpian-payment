package com.alpian.notification;

import com.alpian.payment.events.v1.Money;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import java.util.UUID;

/** Payment events as the payment service publishes them. */
final class Events {

  private Events() {}

  /** A completed payment of CHF 250.50 from {@code accountId}. */
  static PaymentEvent.Builder completed(String accountId) {
    return PaymentEvent.newBuilder()
        .setPaymentId(UUID.randomUUID().toString())
        .setAccountId(accountId)
        .setUserId("11111111-1111-1111-1111-111111111111")
        .setAmount(Money.newBuilder().setAmount("250.5000").setCurrency("CHF"))
        .setStatus(PaymentStatus.PAYMENT_STATUS_COMPLETED)
        .setBeneficiaryName("Acme GmbH")
        .setReference("Invoice 42");
  }
}
