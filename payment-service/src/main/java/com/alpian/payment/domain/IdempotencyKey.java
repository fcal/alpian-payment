package com.alpian.payment.domain;

import java.util.Objects;

/**
 * A client-supplied key identifying one logical payment request.
 *
 * <p>This, not {@link PaymentId}, is what prevents double spending on retry: a client whose request
 * times out cannot know whether the payment was applied, and resending it with the same key
 * resolves to the original outcome instead of debiting twice. Uniqueness is enforced per account by
 * a database constraint, so two accounts may legitimately reuse a key.
 *
 * <p>A key identifies an <em>attempt</em>, not an intent. Replaying the key of a declined payment
 * returns that decline, even if the account has since been funded — the recorded outcome is the
 * answer to "what happened to this request". A client that wants to try again after funding should
 * present a new key. This matches how payment providers generally behave, and keeps the key's
 * meaning to a single well-defined thing.
 */
public record IdempotencyKey(String value) {

  /** Generous enough for a UUID or a client's own composite key, bounded to avoid abuse. */
  public static final int MAX_LENGTH = 255;

  public IdempotencyKey {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("Idempotency key cannot be blank");
    }
    if (value.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Idempotency key exceeds %d characters: %d".formatted(MAX_LENGTH, value.length()));
    }
  }

  @Override
  public String toString() {
    return value;
  }
}
