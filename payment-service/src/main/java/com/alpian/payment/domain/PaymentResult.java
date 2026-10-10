package com.alpian.payment.domain;

/**
 * Outcome of a submission. {@code payment} is the journal entry: the new one for {@code COMPLETED}
 * and {@code INSUFFICIENT_FUNDS}, the original one for {@code REPLAYED}, and null otherwise.
 */
public record PaymentResult(PaymentOutcome outcome, Payment payment) {

  public static PaymentResult rejected(PaymentOutcome outcome) {
    return new PaymentResult(outcome, null);
  }
}
