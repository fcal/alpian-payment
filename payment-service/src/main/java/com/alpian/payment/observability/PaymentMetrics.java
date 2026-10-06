package com.alpian.payment.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Application metrics for the payment path and the outbox relay.
 *
 * <p>Centralised deliberately rather than scattering {@code registry.counter(...)} calls through
 * the code. Metric names are a published interface: dashboards, alert rules and SLOs reference
 * them, so a rename is a breaking change for whoever is on call. Declaring them in one place
 * keeps that interface reviewable, and keeps the naming consistent.
 *
 * <p>Names use Micrometer's dot-separated convention; the Prometheus registry translates them
 * to snake case and appends the relevant suffix, so {@code payment.attempts} is scraped as
 * {@code payment_attempts_total}.
 */
@Component
public class PaymentMetrics {

  private static final String ATTEMPTS = "payment.attempts";
  private static final String AMOUNT = "payment.amount";
  private static final String EXECUTION = "payment.execution";
  private static final String LOCK_WAIT = "payment.lock.wait";
  private static final String OUTBOX_PUBLISHED = "payment.outbox.published";
  private static final String OUTBOX_FAILURES = "payment.outbox.publish.failures";
  private static final String OUTBOX_PARKED = "payment.outbox.parked";
  private static final String OUTBOX_PENDING = "payment.outbox.pending";
  private static final String OUTBOX_OLDEST_AGE = "payment.outbox.oldest.pending.age";

  private final MeterRegistry registry;

  private final Map<PaymentOutcome, Counter> attemptsByOutcome = new EnumMap<>(PaymentOutcome.class);
  private final Map<String, DistributionSummary> amountByCurrency = new ConcurrentHashMap<>();

  private final Timer execution;
  private final Timer lockWait;
  private final Counter outboxPublished;
  private final Counter outboxFailures;
  private final Counter outboxParked;

  /**
   * Backlog gauges are sampled from these holders rather than from a query, so a scrape costs
   * nothing. The relay refreshes them on each poll, which is already a database round trip.
   */
  private final AtomicLong outboxPending = new AtomicLong();

  private final AtomicLong outboxOldestPendingSeconds = new AtomicLong();

  public PaymentMetrics(MeterRegistry registry) {
    this.registry = registry;

    // Every outcome counter is registered up front, at zero. A counter that has never been
    // incremented is absent from the scrape entirely, so an alert such as
    // `rate(payment_attempts_total{outcome="insufficient_funds"}[5m])` would evaluate against a
    // missing series until the first occurrence -- which is exactly when it must already work.
    for (PaymentOutcome outcome : PaymentOutcome.values()) {
      attemptsByOutcome.put(
          outcome,
          Counter.builder(ATTEMPTS)
              .description("Payment attempts by terminal outcome")
              .tag("outcome", outcome.tagValue())
              .register(registry));
    }

    this.execution =
        Timer.builder(EXECUTION)
            .description(
                "Duration of the transactional debit, measured from lock acquisition to commit")
            .register(registry);

    // Separated from execution on purpose. Both rising means the database is slow; only the
    // wait rising means contention on a hot account. Conflating them hides which it is, at the
    // point in an incident where that is the only question that matters.
    this.lockWait =
        Timer.builder(LOCK_WAIT)
            .description("Time spent waiting to acquire the per-account row lock")
            .register(registry);

    this.outboxPublished =
        Counter.builder(OUTBOX_PUBLISHED)
            .description("Outbox rows successfully published to Kafka")
            .register(registry);

    this.outboxFailures =
        Counter.builder(OUTBOX_FAILURES)
            .description("Failed attempts to publish an outbox row")
            .register(registry);

    this.outboxParked =
        Counter.builder(OUTBOX_PARKED)
            .description("Outbox rows parked after exhausting the retry budget")
            .register(registry);

    // The primary service level indicator. A stalled relay is invisible from the API -- payments
    // keep returning 201 while no notification is ever sent -- so backlog depth and the age of
    // the oldest unpublished row are the only signals that detect it. Age matters more than
    // depth: a deep but draining backlog is healthy, a shallow but static one is not.
    Gauge.builder(OUTBOX_PENDING, outboxPending, AtomicLong::get)
        .description("Outbox rows awaiting publication")
        .strongReference(true)
        .register(registry);

    Gauge.builder(OUTBOX_OLDEST_AGE, outboxOldestPendingSeconds, AtomicLong::get)
        .description("Age of the oldest unpublished outbox row")
        .baseUnit("seconds")
        .strongReference(true)
        .register(registry);
  }

  /** Records the terminal outcome of a payment attempt. */
  public void recordAttempt(PaymentOutcome outcome) {
    attemptsByOutcome.get(outcome).increment();
  }

  /**
   * Records the value of a completed payment, tagged by currency.
   *
   * <p>Tagged rather than aggregated because summing across currencies produces a number that
   * means nothing. Cardinality is safe: the tag is bounded by ISO 4217, and in practice by the
   * currencies the accounts actually hold.
   */
  public void recordAmount(String currency, BigDecimal amount) {
    amountByCurrency
        .computeIfAbsent(
            currency,
            c ->
                DistributionSummary.builder(AMOUNT)
                    .description("Value of completed payments")
                    .tag("currency", c)
                    .register(registry))
        .record(amount.doubleValue());
  }

  /** Times the transactional debit. */
  public <T> T recordExecution(Supplier<T> debit) {
    return execution.record(debit);
  }

  /** Records how long a payment waited for the per-account row lock. */
  public void recordLockWait(Duration waited) {
    lockWait.record(waited);
  }

  /** Records a batch of outbox rows published successfully. */
  public void recordOutboxPublished(int count) {
    outboxPublished.increment(count);
  }

  /** Records a failed publication attempt for a single outbox row. */
  public void recordOutboxPublishFailure() {
    outboxFailures.increment();
  }

  /** Records an outbox row parked after exhausting its retry budget. */
  public void recordOutboxParked() {
    outboxParked.increment();
  }

  /**
   * Refreshes the backlog gauges. Called by the relay on each poll.
   *
   * @param pending rows awaiting publication
   * @param oldestAge age of the oldest such row, or {@link Duration#ZERO} when none are pending
   */
  public void updateOutboxBacklog(long pending, Duration oldestAge) {
    outboxPending.set(pending);
    outboxOldestPendingSeconds.set(oldestAge.toSeconds());
  }
}
