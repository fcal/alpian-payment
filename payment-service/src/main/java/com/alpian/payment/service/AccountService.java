package com.alpian.payment.service;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.PaymentRepository;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * Read-side queries on accounts and their payments, scoped to the requesting user.
 *
 * <p>Every query is answered only for an account the user owns, and returns empty — never an error
 * distinguishing the cases — when the account is missing <em>or</em> belongs to someone else. The
 * same rule the payment path applies, for the same reason: confirming that an account or payment
 * exists is itself a disclosure.
 */
@Service
public class AccountService {

  private final AccountRepository accounts;
  private final PaymentRepository payments;

  public AccountService(AccountRepository accounts, PaymentRepository payments) {
    this.accounts = accounts;
    this.payments = payments;
  }

  /** The account, if it exists and belongs to {@code userId}. */
  public Optional<Account> ownedAccount(UserId userId, AccountId accountId) {
    return accounts.findById(accountId).filter(account -> account.isOwnedBy(userId));
  }

  /**
   * A payment, if it was made from {@code accountId} and that account belongs to {@code userId}.
   *
   * <p>The account match matters as much as ownership: without it, a user could read any payment by
   * pairing its id with one of their <em>own</em> accounts in the path. It is part of the query, so
   * a payment on another account is never loaded in the first place.
   */
  public Optional<Payment> payment(UserId userId, AccountId accountId, PaymentId paymentId) {
    return ownedAccount(userId, accountId)
        .flatMap(account -> payments.findByIdAndAccountId(paymentId, accountId));
  }
}
