package com.alpian.payment.domain;

import java.util.Objects;
import java.util.Optional;

/**
 * The result of submitting a payment request.
 *
 * <p>Sealed, so the API layer's mapping to an HTTP response is an exhaustive switch the compiler
 * checks: adding a variant later cannot silently fall through to a default and return the wrong
 * status.
 *
 * <p>The distinction that matters is between {@link Declined} and {@link Rejected}. A decline is a
 * payment that was attempted and recorded in the journal; a rejection never became an attempt, so
 * there is nothing to record. Both are failures to the caller, but only one leaves a trace.
 */
public sealed interface PaymentResult {

  PaymentOutcome outcome();

  /**
   * The journal entry, where one exists.
   *
   * <p>Named for the journal rather than {@code payment()} so that it does not collide with the
   * record components below, which are named for what each variant actually holds.
   */
  Optional<Payment> journalEntry();

  /** Funds were debited and the payment recorded. */
  record Completed(Payment payment) implements PaymentResult {
    public Completed {
      Objects.requireNonNull(payment, "payment");
      if (!payment.isCompleted()) {
        throw new IllegalArgumentException("Expected a completed payment, got " + payment.status());
      }
    }

    @Override
    public PaymentOutcome outcome() {
      return PaymentOutcome.COMPLETED;
    }

    @Override
    public Optional<Payment> journalEntry() {
      return Optional.of(payment);
    }
  }

  /**
   * The payment was attempted, recorded as failed, and no funds moved.
   *
   * <p>Recorded rather than discarded so the journal accounts for every request, and so replaying
   * the idempotency key has a stored outcome to return.
   */
  record Declined(Payment attempt, PaymentOutcome outcome) implements PaymentResult {
    public Declined {
      Objects.requireNonNull(attempt, "attempt");
      Objects.requireNonNull(outcome, "outcome");
      if (attempt.isCompleted()) {
        throw new IllegalArgumentException("A declined result cannot hold a completed payment");
      }
      if (outcome.journalStatus().orElse(null) != PaymentStatus.FAILED) {
        throw new IllegalArgumentException(
            "Outcome %s does not produce a failed journal entry".formatted(outcome));
      }
    }

    @Override
    public Optional<Payment> journalEntry() {
      return Optional.of(attempt);
    }
  }

  /**
   * The request never became a payment attempt — an unknown account, a currency mismatch, a
   * malformed field. Nothing is written to the journal.
   *
   * @param detail a message safe to return to the caller; it must not disclose state the caller is
   *     not entitled to, such as another user's balance
   */
  record Rejected(PaymentOutcome outcome, String detail) implements PaymentResult {
    public Rejected {
      Objects.requireNonNull(outcome, "outcome");
      Objects.requireNonNull(detail, "detail");
      if (outcome.journalStatus().isPresent()) {
        throw new IllegalArgumentException(
            "Outcome %s produces a journal entry and is not a rejection".formatted(outcome));
      }
    }

    @Override
    public Optional<Payment> journalEntry() {
      return Optional.empty();
    }
  }

  /**
   * The idempotency key had already been used, so the stored attempt is returned unchanged and
   * nothing was executed.
   *
   * <p>The replayed payment may itself be completed or failed: a replay reports what happened the
   * first time, it is not an outcome in its own right.
   */
  record Replayed(Payment original) implements PaymentResult {
    public Replayed {
      Objects.requireNonNull(original, "original");
    }

    @Override
    public PaymentOutcome outcome() {
      return PaymentOutcome.IDEMPOTENT_REPLAY;
    }

    @Override
    public Optional<Payment> journalEntry() {
      return Optional.of(original);
    }
  }
}
