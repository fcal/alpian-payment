package com.alpian.payment.service;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.DebitResult;
import com.alpian.payment.repository.PaymentRepository;
import java.time.Clock;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional core of a payment: lock, check, debit, journal — all or nothing.
 *
 * <p>A separate bean from {@link PaymentService} rather than a method on it, for two reasons that
 * both come down to the transaction boundary being in the wrong place otherwise.
 *
 * <p>First, {@code @Transactional} is applied by a proxy, so a call from one method of a bean to
 * another method of the <em>same</em> bean bypasses it entirely. The annotation would be silently
 * inert — the classic Spring self-invocation trap, and particularly dangerous here because an inert
 * transaction also disables the row lock (see {@code requireTransaction} in the repository).
 *
 * <p>Second, and less obviously, some outcomes cannot be handled inside the transaction at all. A
 * unique-constraint violation aborts the PostgreSQL transaction: every subsequent statement on that
 * connection fails until it is rolled back. So the losing side of an idempotency race cannot read
 * the winner's row here, and a lock timeout cannot be turned into a return value here. Both have to
 * propagate out and be handled by the caller, after this transaction has ended.
 *
 * <p>Why the debit and the journal append must share one transaction: a debit that commits while
 * its journal entry does not would remove money with no record of where it went, and the reverse
 * would record a payment that never happened. Neither is acceptable in a ledger, so they commit
 * together or not at all.
 */
@Component
public class PaymentExecutor {

  private static final Logger log = LoggerFactory.getLogger(PaymentExecutor.class);

  private final AccountRepository accounts;
  private final PaymentRepository payments;
  private final Clock clock;

  public PaymentExecutor(AccountRepository accounts, PaymentRepository payments, Clock clock) {
    this.accounts = accounts;
    this.payments = payments;
    this.clock = clock;
  }

  /**
   * Executes the payment in a single transaction.
   *
   * <p>{@code REQUIRES_NEW} is deliberately not used; this must be the outermost boundary for the
   * payment so the row lock is held for the shortest possible window — the lock lives until commit,
   * and anything else on the same connection extends it.
   *
   * @throws com.alpian.payment.repository.DuplicateIdempotencyKeyException if the key was claimed
   *     concurrently. The transaction is doomed at that point; the caller resolves it.
   * @throws org.springframework.dao.CannotAcquireLockException if the row lock was not obtained
   *     within {@code payment.lock-timeout}.
   */
  @Transactional(propagation = Propagation.REQUIRED)
  public PaymentResult execute(PaymentRequest request, PaymentId paymentId) {
    // Ownership is established before anything is read on the account's behalf -- including the
    // replay lookup. In the other order, a user addressing someone else's account with a key that
    // account had used would be handed the stored payment as a "replay".
    Optional<Account> maybeAccount = accounts.findById(request.accountId());
    if (maybeAccount.isEmpty()) {
      return new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_FOUND, "Account not found");
    }

    Account account = maybeAccount.get();
    if (!account.isOwnedBy(request.userId())) {
      log.warn(
          "Account {} addressed by user {} who does not own it",
          request.accountId(),
          request.userId());
      // Reported as "not found": confirming the account exists but belongs to someone else is
      // itself a disclosure. The distinction is kept in the metric and this log, where it is
      // useful, and dropped from the response, where it leaks.
      return new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_OWNED, "Account not found");
    }

    // Inside the transaction, so a replay found here is committed data. It is still not
    // authoritative under concurrency -- two requests can both see nothing -- and the unique
    // constraint on append() is what settles that.
    Optional<Payment> alreadyRecorded =
        payments.findByAccountIdAndIdempotencyKey(request.accountId(), request.idempotencyKey());
    if (alreadyRecorded.isPresent()) {
      return PaymentExecutor.replayOrRefuse(request, alreadyRecorded.get());
    }

    // Checked here only to produce a message naming both currencies; the debit checks it again
    // under the lock, which is the authoritative check.
    if (!account.balance().hasSameCurrencyAs(request.amount())) {
      return new PaymentResult.Rejected(
          PaymentOutcome.CURRENCY_MISMATCH,
          "Payment currency %s does not match account currency %s"
              .formatted(request.amount().currencyCode(), account.balance().currencyCode()));
    }

    return applyDebit(request, paymentId, account.id());
  }

  private PaymentResult applyDebit(PaymentRequest request, PaymentId paymentId, AccountId id) {
    DebitResult debit = accounts.debit(id, request.amount());

    return switch (debit) {
      case DebitResult.Applied applied -> {
        log.info(
            "Debited {} from account {}, balance now {}",
            request.amount(),
            id,
            applied.newBalance());
        yield new PaymentResult.Completed(
            payments.append(
                Payment.completed(
                    paymentId,
                    id,
                    request.idempotencyKey(),
                    request.amount(),
                    request.beneficiary(),
                    request.reference(),
                    clock.instant())));
      }

      case DebitResult.InsufficientFunds insufficient -> {
        log.info(
            "Declined payment of {} on account {}: available {}",
            request.amount(),
            id,
            insufficient.available());
        // Journalled rather than discarded, so every request is accounted for and replaying the
        // key has a stored outcome. The reason quotes no figures: a message is a poor channel for
        // balance disclosure.
        yield new PaymentResult.Declined(
            payments.append(
                Payment.failed(
                    paymentId,
                    id,
                    request.idempotencyKey(),
                    request.amount(),
                    request.beneficiary(),
                    request.reference(),
                    "Insufficient funds",
                    clock.instant())),
            PaymentOutcome.INSUFFICIENT_FUNDS);
      }

        // Already excluded above, so reaching either means the account changed between the read and
        // the lock. The repository's view wins: it is the one that held the lock.
      case DebitResult.AccountNotFound ignored ->
          new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_FOUND, "Account not found");

      case DebitResult.CurrencyMismatch mismatch ->
          new PaymentResult.Rejected(
              PaymentOutcome.CURRENCY_MISMATCH,
              "Payment currency %s does not match account currency %s"
                  .formatted(
                      request.amount().currencyCode(), mismatch.accountBalance().currencyCode()));
    };
  }

  /**
   * Resolves a request whose key has already been used.
   *
   * <p>A key identifies one request, so a repeat is a replay only if it describes the same payment.
   * Replaying a stored outcome for a <em>different</em> request would tell the client that a
   * payment it never made had succeeded — or that one which might have succeeded had failed.
   */
  static PaymentResult replayOrRefuse(PaymentRequest request, Payment recorded) {
    if (!request.describes(recorded)) {
      log.warn(
          "Idempotency key {} reused on account {} for a different payment",
          request.idempotencyKey(),
          request.accountId());
      return new PaymentResult.Rejected(
          PaymentOutcome.IDEMPOTENCY_KEY_REUSED,
          "Idempotency key has already been used for a different payment");
    }
    log.info("Idempotent replay of key {}", request.idempotencyKey());
    return new PaymentResult.Replayed(recorded);
  }
}
