package com.alpian.payment.domain;

import java.util.Locale;

/** What happened to a payment request. {@link #code()} is used in error responses and metrics. */
public enum PaymentOutcome {
  COMPLETED,
  INSUFFICIENT_FUNDS,
  REPLAYED,
  ACCOUNT_NOT_FOUND,
  CURRENCY_MISMATCH,
  IDEMPOTENCY_KEY_REUSED;

  public String code() {
    return name().toLowerCase(Locale.ROOT);
  }
}
