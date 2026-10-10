package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentRequest;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.support.ApplicationTestBase;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;

/** Double-spending guarantees, with real transactions and row locks. */
class PaymentConcurrencyTest extends ApplicationTestBase {

  private static final int THREADS = 32;

  @Autowired PaymentService service;
  @Autowired DataSource dataSource;

  @Test
  @DisplayName("32 concurrent payments never overdraw, and the balance reconciles with the journal")
  void neverOverdraws() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    List<PaymentResult> results = runConcurrently(i -> f.request("100.00", "key-" + i));

    assertThat(results).filteredOn(r -> r.outcome() == PaymentOutcome.COMPLETED).hasSize(10);
    assertThat(results)
        .filteredOn(r -> r.outcome() == PaymentOutcome.INSUFFICIENT_FUNDS)
        .hasSize(THREADS - 10);
    assertThat(balance(f.account())).isZero();
    assertThat(payments(f.account())).isEqualTo(THREADS);
    assertThat(outboxRows(f.account())).isEqualTo(THREADS);
  }

  @Test
  @DisplayName("concurrent retries sharing one idempotency key debit exactly once")
  void concurrentRetriesDebitOnce() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    List<PaymentResult> results = runConcurrently(i -> f.request("100.00", "shared-key"));

    assertThat(results).filteredOn(r -> r.outcome() == PaymentOutcome.COMPLETED).hasSize(1);
    assertThat(results)
        .filteredOn(r -> r.outcome() == PaymentOutcome.REPLAYED)
        .hasSize(THREADS - 1);
    assertThat(balance(f.account())).isEqualByComparingTo("900.00");
    assertThat(outboxRows(f.account())).isEqualTo(1);
  }

  @Test
  @DisplayName("a locked account times out without changes, while other accounts proceed")
  void lockTimeoutIsBoundedAndPerAccount() throws Exception {
    Fixture busy = givenAccount("100.00", "CHF");
    Fixture free = givenAccount("100.00", "CHF");

    try (Connection holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (PreparedStatement lock =
          holder.prepareStatement("SELECT 1 FROM account WHERE id = ? FOR UPDATE")) {
        lock.setObject(1, busy.account());
        lock.execute();
      }

      assertThatThrownBy(() -> service.submit(busy.request("10.00", "blocked")))
          .isInstanceOf(CannotAcquireLockException.class);
      assertThat(service.submit(free.request("10.00", "free")).outcome())
          .isEqualTo(PaymentOutcome.COMPLETED);
      holder.rollback();
    }

    assertThat(balance(busy.account())).isEqualByComparingTo("100.00");
    assertThat(payments(busy.account())).isZero();
  }

  @Test
  @DisplayName("the database refuses to overdraw even if the application tries")
  void databaseConstraintIsTheFinalGuard() {
    Fixture f = givenAccount("100.00", "CHF");

    assertThatThrownBy(
            () ->
                jdbc.sql("UPDATE account SET balance = balance - 500 WHERE id = :id")
                    .param("id", f.account())
                    .update())
        .hasMessageContaining("account_balance_non_negative");
    assertThat(balance(f.account())).isEqualTo(new BigDecimal("100.0000"));
  }

  /** Starts every request at once from a barrier, so the interleaving is real. */
  private List<PaymentResult> runConcurrently(IntFunction<PaymentRequest> request)
      throws Exception {
    CyclicBarrier start = new CyclicBarrier(THREADS);
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      List<Future<PaymentResult>> futures = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        PaymentRequest r = request.apply(i);
        Callable<PaymentResult> task =
            () -> {
              start.await(10, TimeUnit.SECONDS);
              return service.submit(r);
            };
        futures.add(pool.submit(task));
      }
      List<PaymentResult> results = new ArrayList<>();
      for (Future<PaymentResult> future : futures) {
        results.add(future.get(60, TimeUnit.SECONDS));
      }
      return results;
    } finally {
      pool.shutdownNow();
    }
  }
}
