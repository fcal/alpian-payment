package com.alpian.payment.repository;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.IdempotencyKey;

/**
 * Thrown when a payment is saved under an idempotency key the account has already used.
 *
 * <p>Expected under concurrency rather than exceptional: two simultaneous requests carrying the
 * same key can both pass the replay check before either has written, and the unique constraint is
 * what resolves the race. The service treats it as a signal to re-read and return the winner's
 * outcome, which is why it is a distinct type rather than a generic persistence failure.
 */
public class DuplicateIdempotencyKeyException extends RuntimeException {

  private final AccountId accountId;
  private final IdempotencyKey idempotencyKey;

  public DuplicateIdempotencyKeyException(AccountId accountId, IdempotencyKey idempotencyKey) {
    super("Account %s has already used idempotency key %s".formatted(accountId, idempotencyKey));
    this.accountId = accountId;
    this.idempotencyKey = idempotencyKey;
  }

  public AccountId accountId() {
    return accountId;
  }

  public IdempotencyKey idempotencyKey() {
    return idempotencyKey;
  }
}
