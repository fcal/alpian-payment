package com.alpian.payment.repository;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import java.util.List;
import java.util.Optional;

/**
 * The payment journal.
 *
 * <p>Append-only: there is deliberately no update or delete. A payment records something that
 * happened, and history is not editable. A correction is a new entry, not an amendment.
 */
public interface PaymentRepository {

  /**
   * Appends a payment to the journal.
   *
   * @throws DuplicateIdempotencyKeyException if the account has already used that key. This is the
   *     authoritative double-spend guard: unlike the service's prior replay check, it cannot be
   *     raced.
   */
  Payment append(Payment payment);

  /**
   * Finds a payment made from a given account.
   *
   * <p>Deliberately no unscoped {@code findById}. Every read of a payment states which account it
   * belongs to, so the scoping is part of the query rather than a filter a caller might forget —
   * and another account's payment is never loaded at all, rather than loaded and discarded.
   */
  Optional<Payment> findByIdAndAccountId(PaymentId id, AccountId accountId);

  /** Finds a previously recorded attempt under the same key, for idempotent replay. */
  Optional<Payment> findByAccountIdAndIdempotencyKey(AccountId accountId, IdempotencyKey key);

  /** Payment history for an account, most recent first. */
  List<Payment> findByAccountId(AccountId accountId);
}
