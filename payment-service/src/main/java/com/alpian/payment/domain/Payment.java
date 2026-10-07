package com.alpian.payment.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A recorded payment attempt.
 *
 * <p>Entries are a journal: never updated, never deleted. Declined attempts are recorded too, so
 * the journal answers "what happened to this request" for every request, and so the replay of an
 * idempotency key has a stored outcome to return.
 */
public record Payment(
    PaymentId id,
    AccountId accountId,
    IdempotencyKey idempotencyKey,
    Money amount,
    Beneficiary beneficiary,
    String reference,
    PaymentStatus status,
    String failureReason,
    Instant createdAt) {

  public Payment {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(accountId, "accountId");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey");
    Objects.requireNonNull(amount, "amount");
    Objects.requireNonNull(beneficiary, "beneficiary");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(createdAt, "createdAt");

    if (!amount.isPositive()) {
      throw new IllegalArgumentException("Payment amount must be positive: " + amount);
    }
    // Mirrors the payment_failure_reason_matches_status database constraint, so the invariant
    // cannot be violated in memory either and a test does not need a database to catch it.
    if (status == PaymentStatus.FAILED && failureReason == null) {
      throw new IllegalArgumentException("A failed payment must carry a failure reason");
    }
    if (status == PaymentStatus.COMPLETED && failureReason != null) {
      throw new IllegalArgumentException(
          "A completed payment must not carry a failure reason: " + failureReason);
    }
  }

  /** Records a payment whose funds were debited. */
  public static Payment completed(
      PaymentId id,
      AccountId accountId,
      IdempotencyKey idempotencyKey,
      Money amount,
      Beneficiary beneficiary,
      String reference,
      Instant createdAt) {
    return new Payment(
        id,
        accountId,
        idempotencyKey,
        amount,
        beneficiary,
        reference,
        PaymentStatus.COMPLETED,
        null,
        createdAt);
  }

  /** Records an attempt that was declined on business grounds. */
  public static Payment failed(
      PaymentId id,
      AccountId accountId,
      IdempotencyKey idempotencyKey,
      Money amount,
      Beneficiary beneficiary,
      String reference,
      String failureReason,
      Instant createdAt) {
    return new Payment(
        id,
        accountId,
        idempotencyKey,
        amount,
        beneficiary,
        reference,
        PaymentStatus.FAILED,
        Objects.requireNonNull(failureReason, "failureReason"),
        createdAt);
  }

  public boolean isCompleted() {
    return status == PaymentStatus.COMPLETED;
  }

  /** Present only for a failed payment. */
  public Optional<String> failureReasonIfAny() {
    return Optional.ofNullable(failureReason);
  }

  /** Present only when the payer supplied one. */
  public Optional<String> referenceIfAny() {
    return Optional.ofNullable(reference);
  }
}
