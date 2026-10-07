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

  Optional<Payment> findById(PaymentId id);

  /** Finds a previously recorded attempt under the same key, for idempotent replay. */
  Optional<Payment> findByAccountIdAndIdempotencyKey(AccountId accountId, IdempotencyKey key);

  /** Payment history for an account, most recent first. */
  List<Payment> findByAccountId(AccountId accountId);
}
