package com.alpian.payment.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PaymentMetricsTest {

  private MeterRegistry registry;
  private PaymentMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new PaymentMetrics(registry);
  }

  @ParameterizedTest
  @EnumSource(PaymentOutcome.class)
  @DisplayName("every outcome is registered at zero before anything happens")
  void registersEveryOutcomeUpFront(PaymentOutcome outcome) {
    // Guards the property that makes alerting work: an un-incremented counter is absent from a
    // Prometheus scrape, so a rule filtering on a rare outcome would have no series to evaluate
    // until that outcome first occurred -- precisely when the alert needs to already exist.
    Counter counter = registry.find("payment.attempts").tag("outcome", outcome.tagValue()).counter();

    assertThat(counter).as("counter for outcome %s", outcome).isNotNull();
    assertThat(counter.count()).isZero();
  }

  @Test
  @DisplayName("outcome tag values are unique, so no two outcomes collide into one series")
  void outcomeTagValuesAreUnique() {
    long distinct =
        Arrays.stream(PaymentOutcome.values()).map(PaymentOutcome::tagValue).distinct().count();

    assertThat(distinct).isEqualTo(PaymentOutcome.values().length);
  }

  @Test
  void countsAttemptsPerOutcomeIndependently() {
    metrics.recordAttempt(PaymentOutcome.COMPLETED);
    metrics.recordAttempt(PaymentOutcome.COMPLETED);
    metrics.recordAttempt(PaymentOutcome.INSUFFICIENT_FUNDS);

    assertThat(counterFor(PaymentOutcome.COMPLETED)).isEqualTo(2);
    assertThat(counterFor(PaymentOutcome.INSUFFICIENT_FUNDS)).isEqualTo(1);
    assertThat(counterFor(PaymentOutcome.LOCK_TIMEOUT)).isZero();
  }

  @Test
  @DisplayName("amounts are kept in separate series per currency")
  void separatesAmountsByCurrency() {
    metrics.recordAmount("CHF", new BigDecimal("100.50"));
    metrics.recordAmount("CHF", new BigDecimal("10.00"));
    metrics.recordAmount("EUR", new BigDecimal("7.25"));

    var chf = registry.find("payment.amount").tag("currency", "CHF").summary();
    var eur = registry.find("payment.amount").tag("currency", "EUR").summary();

    assertThat(chf).isNotNull();
    assertThat(chf.count()).isEqualTo(2);
    assertThat(chf.totalAmount()).isEqualTo(110.50);

    assertThat(eur).isNotNull();
    assertThat(eur.count()).isEqualTo(1);
    assertThat(eur.totalAmount()).isEqualTo(7.25);
  }

  @Test
  void timesExecutionAndReturnsTheResult() {
    String result = metrics.recordExecution(() -> "debited");

    assertThat(result).isEqualTo("debited");
    var timer = registry.find("payment.execution").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  @DisplayName("lock wait is a separate timer from execution")
  void recordsLockWaitSeparatelyFromExecution() {
    // The two must not share a series: both rising means a slow database, only the wait rising
    // means contention on a hot account, and that distinction is the whole diagnostic value.
    metrics.recordLockWait(Duration.ofMillis(40));

    assertThat(registry.find("payment.lock.wait").timer().count()).isEqualTo(1);
    assertThat(registry.find("payment.execution").timer().count()).isZero();
  }

  @Test
  void tracksOutboxPublicationCounters() {
    metrics.recordOutboxPublished(5);
    metrics.recordOutboxPublished(3);
    metrics.recordOutboxPublishFailure();
    metrics.recordOutboxParked();

    assertThat(registry.find("payment.outbox.published").counter().count()).isEqualTo(8);
    assertThat(registry.find("payment.outbox.publish.failures").counter().count()).isEqualTo(1);
    assertThat(registry.find("payment.outbox.parked").counter().count()).isEqualTo(1);
  }

  @Test
  @DisplayName("backlog gauges reflect the latest relay poll")
  void backlogGaugesTrackLatestValue() {
    assertThat(registry.find("payment.outbox.pending").gauge().value()).isZero();

    metrics.updateOutboxBacklog(42, Duration.ofSeconds(90));

    assertThat(registry.find("payment.outbox.pending").gauge().value()).isEqualTo(42);
    assertThat(registry.find("payment.outbox.oldest.pending.age").gauge().value()).isEqualTo(90);

    // Gauges are absolute, not cumulative: a drained backlog must read zero, otherwise an alert
    // on depth would latch on permanently after one spike.
    metrics.updateOutboxBacklog(0, Duration.ZERO);

    assertThat(registry.find("payment.outbox.pending").gauge().value()).isZero();
    assertThat(registry.find("payment.outbox.oldest.pending.age").gauge().value()).isZero();
  }

  private double counterFor(PaymentOutcome outcome) {
    return registry.find("payment.attempts").tag("outcome", outcome.tagValue()).counter().count();
  }
}
