package com.alpian.payment.repository;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import java.util.Optional;

/**
 * Account persistence.
 *
 * <p>Note that {@link #debit} is not a CRUD operation but a single conditional one. This is
 * deliberate, and is the most consequential shape decision in the codebase.
 *
 * <p>A CRUD interface — {@code findById} then {@code save} — would force every caller into a
 * read-modify-write sequence. Two concurrent requests would then both read a sufficient balance and
 * both debit it, and <em>no implementation could prevent it</em>, because the gap between the read
 * and the write lives in the caller. Expressing the balance check and the debit as one operation
 * moves that gap inside the implementation, where it can be closed with a row lock.
 *
 * <p>The division of responsibility is therefore: this interface guarantees that a debit is atomic
 * and never overdraws; the service layer decides what each outcome <em>means</em> — which error the
 * client sees, what is written to the journal, which metric is incremented. Policy stays in the
 * service, atomicity lives with the storage that can actually provide it.
 */
public interface AccountRepository {

  /** Looks up an account regardless of owner. */
  Optional<Account> findById(AccountId id);

  /**
   * Atomically debits {@code amount} from the account, if and only if the currencies match and the
   * balance covers it in full.
   *
   * <p>Implementations must make the check and the decrement indivisible with respect to other
   * callers, including callers in other processes. Returning {@link DebitResult.InsufficientFunds}
   * must leave the balance untouched.
   *
   * @return what happened, never null
   */
  DebitResult debit(AccountId id, Money amount);

  /**
   * Persists an account. Present for test fixtures and for seeding; not used by the payment path.
   */
  Account save(Account account);

  /** Accounts belonging to a user. */
  java.util.List<Account> findByUserId(UserId userId);
}
