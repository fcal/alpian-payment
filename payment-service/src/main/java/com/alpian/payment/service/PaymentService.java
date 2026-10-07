package com.alpian.payment.service;

import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.DuplicateIdempotencyKeyException;
import com.alpian.payment.repository.PaymentRepository;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.stereotype.Service;

/**
 * Entry point for submitting payments.
 *
 * <p>Deliberately <em>not</em> transactional. It wraps {@link PaymentExecutor}, which is, and
 * handles the two outcomes that can only be dealt with once that transaction has ended: a lost
 * idempotency race and a lock timeout. Both arrive as exceptions from a transaction that PostgreSQL
 * has already aborted, so neither can be turned into a result inside it.
 *
 * <p>This split also keeps metrics and MDC outside the transaction, so instrumentation never
 * extends the window during which the account row lock is held.
 */
@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  /** MDC key, matched by the logging pattern in application.yaml. */
  private static final String MDC_PAYMENT_ID = "paymentId";

  private final PaymentExecutor executor;
  private final PaymentRepository payments;
  private final PaymentMetrics metrics;

  public PaymentService(
      PaymentExecutor executor, PaymentRepository payments, PaymentMetrics metrics) {
    this.executor = executor;
    this.payments = payments;
    this.metrics = metrics;
  }

  /**
   * Submits a payment.
   *
   * <p>Never throws for a business outcome: insufficient funds, an unknown account, a currency
   * mismatch and lock contention are all represented in the returned {@link PaymentResult}.
   * Exceptions are reserved for genuine faults.
   */
  public PaymentResult submit(PaymentRequest request) {
    PaymentId paymentId = PaymentId.generate();
    MDC.put(MDC_PAYMENT_ID, paymentId.toString());
    try {
      PaymentResult result = attempt(request, paymentId);
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

  private PaymentResult attempt(PaymentRequest request, PaymentId paymentId) {
    try {
      return executor.execute(request, paymentId);

    } catch (DuplicateIdempotencyKeyException e) {
      // Two concurrent requests shared a key; this one lost the unique constraint and its
      // transaction, including its debit, has been rolled back. The right answer is the same as
      // for any other replay -- what happened to this key -- not an error.
      log.info(
          "Lost the race on idempotency key {}; returning the recorded outcome",
          request.idempotencyKey());
      return replayWinner(request, e);

    } catch (CannotAcquireLockException e) {
      // lock_timeout elapsed waiting for the account row, so another payment on this account is
      // in flight. Nothing was changed. Surfaced as a distinct outcome so the API can answer with
      // a retryable status rather than a generic failure -- the request is not invalid, merely
      // unlucky in its timing.
      log.warn("Timed out waiting for the lock on account {}", request.accountId());
      return new PaymentResult.Rejected(
          PaymentOutcome.LOCK_TIMEOUT, "Another payment on this account is in progress");
    }
  }

  /**
   * Reads the winning attempt.
   *
   * <p>Needs no transaction of its own, and must not be annotated with one. By the time control
   * reaches here the exception has already propagated through {@link PaymentExecutor}'s
   * transactional proxy, which rolled the doomed transaction back and released its connection — so
   * this runs on a clean connection, and a single statement in autocommit is its own transaction
   * and sees the winner's committed row.
   *
   * <p>An earlier version of this method carried {@code @Transactional(REQUIRES_NEW)}, which would
   * have been silently inert: it is invoked from {@link #attempt} on {@code this}, and
   * self-invocation bypasses the proxy that applies the annotation. Worth noting as the same trap
   * documented on {@link PaymentExecutor} — an annotation that appears to promise a boundary while
   * providing none is worse than no annotation, so it is kept private and the reasoning explicit.
   */
  private PaymentResult replayWinner(
      PaymentRequest request, DuplicateIdempotencyKeyException cause) {
    Optional<Payment> winner =
        payments.findByAccountIdAndIdempotencyKey(request.accountId(), request.idempotencyKey());
    return winner
        .<PaymentResult>map(PaymentResult.Replayed::new)
        // The constraint fired, so a committed row must exist by the time this new transaction
        // starts. Absent means an assumption is broken somewhere; rethrowing surfaces that rather
        // than inventing a result.
        .orElseThrow(() -> cause);
  }
}
