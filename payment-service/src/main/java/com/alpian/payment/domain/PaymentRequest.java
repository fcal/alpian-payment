package com.alpian.payment.domain;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/** A request to pay out of an account, normalised so that equivalent retries compare equal. */
public record PaymentRequest(
    UUID userId,
    UUID accountId,
    String idempotencyKey,
    BigDecimal amount,
    String currency,
    String beneficiaryName,
    String beneficiaryIban,
    String reference) {

  public PaymentRequest {
    amount = amount.setScale(Payment.SCALE);
    beneficiaryName = beneficiaryName.strip();
    beneficiaryIban = beneficiaryIban.replace(" ", "").toUpperCase(Locale.ROOT);
    if (reference != null && reference.isBlank()) {
      reference = null;
    }
  }

  /** Whether {@code payment} was recorded for this same request, so replaying it is correct. */
  public boolean matches(Payment payment) {
    return payment.amount().compareTo(amount) == 0
        && payment.currency().equals(currency)
        && payment.beneficiaryName().equals(beneficiaryName)
        && payment.beneficiaryIban().equals(beneficiaryIban)
        && Objects.equals(payment.reference(), reference);
  }
}
