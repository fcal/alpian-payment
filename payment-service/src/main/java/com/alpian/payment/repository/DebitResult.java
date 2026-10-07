package com.alpian.payment.repository;

import com.alpian.payment.domain.Money;
import java.util.Objects;

/**
 * The outcome of attempting to debit an account.
 *
 * <p>Sealed rather than an exception or a boolean, so the caller must handle each case and gets the
 * balance back on the insufficient-funds path without a second read — which would be racy anyway,
 * since the lock is released at commit.
 */
public sealed interface DebitResult {

  /** The debit was applied. */
  record Applied(Money newBalance) implements DebitResult {
    public Applied {
      Objects.requireNonNull(newBalance, "newBalance");
    }
  }

  /**
   * The account holds less than the requested amount; nothing was changed.
   *
   * @param available the balance observed under the lock, so it is the balance the decision was
   *     actually made against
   */
  record InsufficientFunds(Money available) implements DebitResult {
    public InsufficientFunds {
      Objects.requireNonNull(available, "available");
    }
  }

  /** No such account. Distinct from a zero balance. */
  record AccountNotFound() implements DebitResult {}

  /**
   * The payment currency does not match the account currency. Checked inside the debit because only
   * there is the account's currency known under the lock.
   */
  record CurrencyMismatch(Money accountBalance) implements DebitResult {
    public CurrencyMismatch {
      Objects.requireNonNull(accountBalance, "accountBalance");
    }
  }
}
