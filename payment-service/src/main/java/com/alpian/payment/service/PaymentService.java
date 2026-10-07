package com.alpian.payment.service;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.DebitResult;
import com.alpian.payment.repository.DuplicateIdempotencyKeyException;
import com.alpian.payment.repository.PaymentRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

/**
 * Executes outbound payments.
 *
 * <p>Owns the decisions: whether a request is permissible, which outcome it produces, and what is
 * written to the journal. It does <em>not</em> own atomicity — the balance check and the debit are
 * one indivisible operation provided by {@link AccountRepository#debit}, because a check performed
 * here and a write performed there would leave a race no implementation could close. See that
 * method for the full reasoning.
 *
 * <p>Transactional boundaries are deliberately absent at this stage. In the PostgreSQL
 * implementation the debit and the journal append must commit together, so this method becomes
 * {@code @Transactional}: a journal append that fails after a successful debit has to roll the
 * debit back, or funds would vanish without a record. The in-memory implementation used by these
 * tests cannot reproduce that, which is precisely why the concurrency guarantees are verified
 * against a real database instead.
 */
@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  /** MDC key, matched by the logging pattern in application.yaml. */
  private static final String MDC_PAYMENT_ID = "paymentId";

  private final AccountRepository accounts;
  private final PaymentRepository payments;
  private final PaymentMetrics metrics;
  private final Clock clock;

  public PaymentService(
      AccountRepository accounts, PaymentRepository payments, PaymentMetrics metrics, Clock clock) {
    this.accounts = accounts;
    this.payments = payments;
    this.metrics = metrics;
    this.clock = clock;
  }

  /**
   * Submits a payment.
   *
   * <p>Never throws for a business outcome: insufficient funds, an unknown account and a currency
   * mismatch are all represented in the returned {@link PaymentResult}. Exceptions are reserved for
   * faults — an unreachable database, a programming error.
   */
  public PaymentResult submit(PaymentRequest request) {
    PaymentId paymentId = PaymentId.generate();
    MDC.put(MDC_PAYMENT_ID, paymentId.toString());
    try {
      PaymentResult result = execute(request, paymentId);
      metrics.recordAttempt(result.outcome());
      if (result instanceof PaymentResult.Completed completed) {
        metrics.recordAmount(
            completed.payment().amount().currencyCode(), completed.payment().amount().amount());
      }
      return result;
    } finally {
      MDC.remove(MDC_PAYMENT_ID);
    }
  }

  private PaymentResult execute(PaymentRequest request, PaymentId paymentId) {
    // Replay check first: it is the cheapest path and short-circuits everything else. It is not
    // authoritative, though -- two concurrent requests with the same key can both get past it.
    // The unique constraint on append() is what actually resolves that, handled below.
    Optional<Payment> alreadyRecorded =
        payments.findByAccountIdAndIdempotencyKey(request.accountId(), request.idempotencyKey());
    if (alreadyRecorded.isPresent()) {
      log.info(
          "Idempotent replay of key {} on account {}",
          request.idempotencyKey(),
          request.accountId());
      return new PaymentResult.Replayed(alreadyRecorded.get());
    }

    Optional<Account> maybeAccount = accounts.findById(request.accountId());
    if (maybeAccount.isEmpty()) {
      return new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_FOUND, "Account not found");
    }

    Account account = maybeAccount.get();
    if (!account.isOwnedBy(request.userId())) {
      // Logged at warn because a legitimate client never addresses another user's account: this
      // is either a client defect or probing, and the metric for it is worth alerting on.
      log.warn(
          "Account {} addressed by user {} who does not own it",
          request.accountId(),
          request.userId());
      // Reported as "not found" to the caller rather than "not yours", which would confirm the
      // account exists. The distinction is preserved in the metric and the log, where it is
      // useful, not in the response, where it leaks.
      return new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_OWNED, "Account not found");
    }

    // Checked here so the caller gets a precise message naming both currencies. The repository
    // checks it again under the lock, since that is the only place the account's currency is
    // known to be current -- this check is for the error message, not for correctness.
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
        yield record(
            Payment.completed(
                paymentId,
                id,
                request.idempotencyKey(),
                request.amount(),
                request.beneficiary(),
                request.reference(),
                now()),
            PaymentOutcome.COMPLETED,
            request);
      }

      case DebitResult.InsufficientFunds insufficient -> {
        log.info(
            "Declined payment of {} on account {}: available {}",
            request.amount(),
            id,
            insufficient.available());
        // Recorded in the journal rather than discarded, so every request is accounted for and a
        // replay of the key has a stored outcome. The reason names no figures: the amounts are
        // already known to the caller, and a message is a poor channel for balance disclosure.
        yield record(
            Payment.failed(
                paymentId,
                id,
                request.idempotencyKey(),
                request.amount(),
                request.beneficiary(),
                request.reference(),
                "Insufficient funds",
                now()),
            PaymentOutcome.INSUFFICIENT_FUNDS,
            request);
      }

        // Both of the following were already excluded above, so reaching them means the account
        // changed between the read and the debit. Reported, not crashed: the repository's view is
        // the authoritative one because it held the lock.
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
   * Appends to the journal, resolving a lost race on the idempotency key by returning the winner's
   * outcome.
   *
   * <p>Two concurrent requests with the same key can both pass the replay check. One wins the
   * unique constraint; the other lands here, and the correct answer for it is the same as for any
   * other replay — what happened to this key — rather than an error.
   */
  private PaymentResult record(Payment payment, PaymentOutcome outcome, PaymentRequest request) {
    try {
      Payment appended = payments.append(payment);
      return outcome == PaymentOutcome.COMPLETED
          ? new PaymentResult.Completed(appended)
          : new PaymentResult.Declined(appended, outcome);
    } catch (DuplicateIdempotencyKeyException e) {
      log.info(
          "Lost the race on idempotency key {} for account {}; returning the recorded outcome",
          request.idempotencyKey(),
          request.accountId());
      return payments
          .findByAccountIdAndIdempotencyKey(request.accountId(), request.idempotencyKey())
          .<PaymentResult>map(PaymentResult.Replayed::new)
          // The constraint fired, so a row exists. Absent means a non-repeatable read, which with
          // the real transaction boundaries of stage 2 cannot happen; rethrowing surfaces it
          // loudly rather than inventing a result.
          .orElseThrow(() -> e);
    }
  }

  private Instant now() {
    return clock.instant();
  }
}
