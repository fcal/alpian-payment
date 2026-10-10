package com.alpian.payment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/** A recorded payment attempt. The journal is append-only; declines are recorded too. */
public record Payment(
    UUID id,
    UUID accountId,
    String idempotencyKey,
    BigDecimal amount,
    String currency,
    String beneficiaryName,
    String beneficiaryIban,
    String reference,
    PaymentStatus status,
    String failureReason,
    Instant createdAt) {

  /** Matches the NUMERIC(19,4) columns. */
  public static final int SCALE = 4;

  public static Payment completed(PaymentRequest request) {
    return of(request, PaymentStatus.COMPLETED, null);
  }

  public static Payment declined(PaymentRequest request, String reason) {
    return of(request, PaymentStatus.FAILED, reason);
  }

  private static Payment of(PaymentRequest request, PaymentStatus status, String failureReason) {
    return new Payment(
        UUID.randomUUID(),
        request.accountId(),
        request.idempotencyKey(),
        request.amount(),
        request.currency(),
        request.beneficiaryName(),
        request.beneficiaryIban(),
        request.reference(),
        status,
        failureReason,
        // Postgres stores microseconds; truncating keeps a round trip lossless.
        Instant.now().truncatedTo(ChronoUnit.MICROS));
  }

  public boolean isCompleted() {
    return status == PaymentStatus.COMPLETED;
  }
}
