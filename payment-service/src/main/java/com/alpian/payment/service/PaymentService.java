package com.alpian.payment.service;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentRequest;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.outbox.PaymentEvents;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.OutboxRepository;
import com.alpian.payment.repository.PaymentRepository;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  private final AccountRepository accounts;
  private final PaymentRepository payments;
  private final OutboxRepository outbox;

  public PaymentService(
      AccountRepository accounts, PaymentRepository payments, OutboxRepository outbox) {
    this.accounts = accounts;
    this.payments = payments;
    this.outbox = outbox;
  }

  /**
   * Submits a payment in a single transaction.
   *
   * <p>The account row is locked first, so every check below runs against a state no concurrent
   * payment can change: the balance check cannot be raced (no double spending), and a retry with
   * the same idempotency key waits for the first attempt and then replays it. The debit, the
   * journal entry and the outbox event commit together or not at all.
   *
   * @throws org.springframework.dao.CannotAcquireLockException if another payment holds the account
   *     lock for longer than {@code payment.lock-timeout}; nothing is changed
   */
  @Transactional
  public PaymentResult submit(PaymentRequest request) {
    // Ownership is checked before the replay lookup, so another user's key replays nothing. A
    // foreign account is reported as missing so that its existence is not disclosed.
    Optional<Account> locked =
        accounts.lock(request.accountId()).filter(a -> a.userId().equals(request.userId()));
    if (locked.isEmpty()) {
      return PaymentResult.rejected(PaymentOutcome.ACCOUNT_NOT_FOUND);
    }
    Account account = locked.get();

    Optional<Payment> previous =
        payments.findByIdempotencyKey(request.accountId(), request.idempotencyKey());
    if (previous.isPresent()) {
      if (!request.matches(previous.get())) {
        log.warn("Idempotency key {} reused for a different payment", request.idempotencyKey());
        return PaymentResult.rejected(PaymentOutcome.IDEMPOTENCY_KEY_REUSED);
      }
      return new PaymentResult(PaymentOutcome.REPLAYED, previous.get());
    }

    if (!account.currency().equals(request.currency())) {
      return PaymentResult.rejected(PaymentOutcome.CURRENCY_MISMATCH);
    }

    Payment payment;
    if (account.balance().compareTo(request.amount()) >= 0) {
      accounts.debit(account.id(), request.amount());
      payment = Payment.completed(request);
    } else {
      payment = Payment.declined(request, "Insufficient funds");
    }

    // Declines are journalled and notified too, so a replay has an outcome to return.
    payments.insert(payment);
    outbox.enqueue(
        payment.id(),
        account.id().toString(),
        PaymentEvents.toEvent(payment, request.userId()).toByteArray());
    log.info("Payment {} on account {}: {}", payment.id(), account.id(), payment.status());
    return new PaymentResult(
        payment.isCompleted() ? PaymentOutcome.COMPLETED : PaymentOutcome.INSUFFICIENT_FUNDS,
        payment);
  }

  /** The account, if it belongs to {@code userId}. */
  public Optional<Account> account(UUID userId, UUID accountId) {
    return accounts.find(accountId).filter(a -> a.userId().equals(userId));
  }

  /** A payment made from {@code accountId}, if that account belongs to {@code userId}. */
  public Optional<Payment> payment(UUID userId, UUID accountId, UUID paymentId) {
    return account(userId, accountId).flatMap(a -> payments.find(accountId, paymentId));
  }
}
