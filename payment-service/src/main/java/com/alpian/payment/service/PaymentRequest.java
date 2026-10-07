package com.alpian.payment.service;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import java.util.Objects;

/**
 * A validated request to make an outbound payment.
 *
 * <p>A domain type rather than the API's request body: the two change for different reasons — the
 * wire format for client compatibility, this for business rules — and keeping them separate stops
 * an HTTP concern such as a JSON field name from leaking into the service. The API layer maps one
 * to the other.
 *
 * <p>Constructing an instance establishes that the fields are individually well formed. Whether the
 * payment is <em>permissible</em> — the account exists, belongs to the user, holds the funds — is
 * decided by {@link PaymentService}, since it depends on state.
 *
 * @param userId the paying user; in production this comes from the authenticated principal, not
 *     from the request
 * @param reference optional free-text reference supplied by the payer
 */
public record PaymentRequest(
    UserId userId,
    AccountId accountId,
    IdempotencyKey idempotencyKey,
    Money amount,
    Beneficiary beneficiary,
    String reference) {

  public static final int MAX_REFERENCE_LENGTH = 140;

  public PaymentRequest {
    Objects.requireNonNull(userId, "userId");
    Objects.requireNonNull(accountId, "accountId");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey");
    Objects.requireNonNull(amount, "amount");
    Objects.requireNonNull(beneficiary, "beneficiary");

    // Money already rejects negatives; zero needs rejecting separately. A zero-value payment has
    // no effect but would still consume an idempotency key and emit a notification.
    if (!amount.isPositive()) {
      throw new IllegalArgumentException("Payment amount must be greater than zero: " + amount);
    }
    if (reference != null && reference.length() > MAX_REFERENCE_LENGTH) {
      throw new IllegalArgumentException(
          "Reference exceeds %d characters".formatted(MAX_REFERENCE_LENGTH));
    }
    if (reference != null && reference.isBlank()) {
      // Normalise "present but empty" to absent, so the journal does not distinguish between a
      // missing reference and a whitespace one.
      reference = null;
    }
  }
}
