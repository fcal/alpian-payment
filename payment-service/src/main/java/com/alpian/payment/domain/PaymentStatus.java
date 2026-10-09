package com.alpian.payment.domain;

/** Terminal states only: a payment is applied in one transaction, so it is never pending. */
public enum PaymentStatus {
  COMPLETED,
  FAILED
}
