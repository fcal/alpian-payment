package com.alpian.payment.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.support.MutableClock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PaymentMetricsTest {

  private static final Instant START = Instant.parse("2026-10-08T09:00:00Z");

  private MeterRegistry registry;
  private MutableClock clock;
  private PaymentMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    clock = new MutableClock(START);
    metrics = new PaymentMetrics(registry, clock);
  }

  @ParameterizedTest
  @EnumSource(PaymentOutcome.class)
  @DisplayName("every outcome is registered at zero before anything happens")
  void registersEveryOutcomeUpFront(PaymentOutcome outcome) {
    // Guards the property that makes alerting work: an un-incremented counter is absent from a
    // Prometheus scrape, so a rule filtering on a rare outcome would have no series to evaluate
    // until that outcome first occurred -- precisely when the alert needs to already exist.
    Counter counter =
        registry.find("payment.attempts").tag("outcome", outcome.tagValue()).counter();

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
    assertThat(gauge("payment.outbox.pending")).isZero();

    metrics.updateOutboxBacklog(42, Optional.of(START.minusSeconds(90)));

    assertThat(gauge("payment.outbox.pending")).isEqualTo(42);
    assertThat(gauge("payment.outbox.oldest.pending.age")).isEqualTo(90);

    // Gauges are absolute, not cumulative: a drained backlog must read zero, or an alert on depth
    // would latch on permanently after one spike.
    metrics.updateOutboxBacklog(0, Optional.empty());

    assertThat(gauge("payment.outbox.pending")).isZero();
    assertThat(gauge("payment.outbox.oldest.pending.age")).isZero();
  }

  @Test
  @DisplayName("the oldest-pending age keeps growing when the relay stops updating it")
  void oldestAgeGrowsWhileTheRelayIsStalled() {
    // The reason the gauge holds a timestamp rather than an age. A stalled relay makes no further
    // updates; were the age stored, it would freeze at 30s and the stalled-relay alert -- the
    // highest-value alert in the service -- could never fire.
    metrics.updateOutboxBacklog(5, Optional.of(START.minusSeconds(30)));

    clock.advance(Duration.ofMinutes(10)); // no further updates arrive

    assertThat(gauge("payment.outbox.oldest.pending.age")).isEqualTo(630);
  }

  @Test
  @DisplayName("time since the last relay poll grows when polls stop, even with nothing pending")
  void lastPollAgeCatchesARelayThatDiedOnAnEmptyBacklog() {
    // The case the oldest-age gauge cannot cover: the relay's last observation was an empty
    // backlog, so oldest-age reads zero forever while new rows pile up unseen.
    metrics.updateOutboxBacklog(0, Optional.empty());
    metrics.recordRelayPoll();

    clock.advance(Duration.ofMinutes(5));

    assertThat(gauge("payment.outbox.oldest.pending.age")).isZero();
    assertThat(gauge("payment.outbox.relay.last.poll.age")).isEqualTo(300);

    metrics.recordRelayPoll();
    assertThat(gauge("payment.outbox.relay.last.poll.age")).isZero();
  }

  @Test
  @DisplayName("a relay that never polls at all shows a growing age from startup")
  void lastPollAgeStartsAtStartup() {
    clock.advance(Duration.ofSeconds(45));

    assertThat(gauge("payment.outbox.relay.last.poll.age")).isEqualTo(45);
  }

  private double gauge(String name) {
    return registry.find(name).gauge().value();
  }

  private double counterFor(PaymentOutcome outcome) {
    return registry.find("payment.attempts").tag("outcome", outcome.tagValue()).counter().count();
  }
}
