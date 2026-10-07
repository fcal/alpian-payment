package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.inmemory.InMemoryAccountRepository;
import com.alpian.payment.repository.inmemory.InMemoryPaymentRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Concurrency tests for {@link PaymentService} against the in-memory repositories.
 *
 * <p>These verify that the service is correct <em>given a repository that honours its contract</em>
 * — that it does not reintroduce a race above the repository by, say, checking the balance itself
 * and then debiting. The in-memory double makes its debit atomic per account via {@code
 * ConcurrentHashMap.compute}, which is the single-JVM equivalent of {@code SELECT ... FOR UPDATE}.
 *
 * <p>What these tests cannot show is that the PostgreSQL implementation honours the contract: no
 * in-memory map reproduces row locking across processes, or rollback. The equivalent suite run
 * against a real database in stage 2 is what establishes that, and it is the authoritative one.
 */
class PaymentServiceConcurrencyTest {

  private static final Instant NOW = Instant.parse("2026-10-06T12:00:00Z");
  private static final UserId OWNER = new UserId(UUID.randomUUID());
  private static final AccountId ACCOUNT = new AccountId(UUID.randomUUID());
  private static final Beneficiary BENEFICIARY =
      new Beneficiary("Acme GmbH", "CH9300762011623852957");

  private static final int THREADS = 32;

  private InMemoryAccountRepository accounts;
  private InMemoryPaymentRepository payments;
  private PaymentService service;

  @BeforeEach
  void setUp() {
    Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    accounts = new InMemoryAccountRepository(clock);
    payments = new InMemoryPaymentRepository();
    service =
        new PaymentService(
            new PaymentExecutor(accounts, payments, clock),
            payments,
            new PaymentMetrics(new SimpleMeterRegistry()));
  }

  @Test
  @DisplayName("concurrent payments on one account never overdraw it")
  void neverOverdrawsUnderConcurrency() throws Exception {
    // 32 threads each try to spend 100 from a balance of 1000. At most ten can succeed; the
    // remainder must be declined. A lost update would show up as a negative balance or as more
    // than ten successes.
    accounts.save(new Account(ACCOUNT, OWNER, Money.of("1000.00", "CHF"), NOW, NOW));

    List<PaymentResult> results =
        runConcurrently(i -> () -> service.submit(requestFor("100.00", "concurrent-key-" + i)));

    long completed = results.stream().filter(r -> r instanceof PaymentResult.Completed).count();
    long declined = results.stream().filter(r -> r instanceof PaymentResult.Declined).count();

    assertThat(completed).as("payments that could be funded").isEqualTo(10);
    assertThat(declined).as("payments declined for insufficient funds").isEqualTo(THREADS - 10);

    Money finalBalance = accounts.findById(ACCOUNT).orElseThrow().balance();
    assertThat(finalBalance).as("1000 less ten payments of 100").isEqualTo(Money.of("0.00", "CHF"));

    // Every attempt is accounted for in the journal, successful or not.
    assertThat(payments.size()).isEqualTo(THREADS);
  }

  @Test
  @DisplayName("the balance is exactly consistent with the payments that succeeded")
  void balanceReconcilesWithTheJournal() throws Exception {
    accounts.save(new Account(ACCOUNT, OWNER, Money.of("500.00", "CHF"), NOW, NOW));

    List<PaymentResult> results =
        runConcurrently(i -> () -> service.submit(requestFor("30.00", "mixed-key-" + i)));

    Money debited =
        results.stream()
            .filter(r -> r instanceof PaymentResult.Completed)
            .map(r -> r.journalEntry().orElseThrow().amount())
            .reduce(Money.of("0.00", "CHF"), Money::add);

    // The strongest statement available: whatever interleaving occurred, the money removed from
    // the account equals the money recorded as having left it. No payment went unrecorded, and
    // no record exists without a corresponding debit.
    assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
        .isEqualTo(Money.of("500.00", "CHF").subtract(debited));
  }

  @Test
  @DisplayName("concurrent requests sharing an idempotency key produce exactly one payment")
  void concurrentRetriesOfOneKeyProduceOnePayment() throws Exception {
    // The scenario a server-generated id cannot handle: a client retrying a request whose outcome
    // it never learned, with both attempts in flight at once. The replay check alone cannot
    // resolve it -- every thread passes the check before any has written -- so correctness rests
    // on the unique constraint, and on the service treating the loser as a replay, not an error.
    //
    // The balance is funded well beyond the total so that every debit would individually succeed,
    // making the expected split deterministic: one winner, the rest replays.
    accounts.save(new Account(ACCOUNT, OWNER, Money.of("10000.00", "CHF"), NOW, NOW));
    IdempotencyKey sharedKey = new IdempotencyKey("retried-key");

    List<PaymentResult> results =
        runConcurrently(
            i ->
                () ->
                    service.submit(
                        new PaymentRequest(
                            OWNER,
                            ACCOUNT,
                            sharedKey,
                            Money.of("100.00", "CHF"),
                            BENEFICIARY,
                            null)));

    long executed = results.stream().filter(r -> r instanceof PaymentResult.Completed).count();
    long replayed = results.stream().filter(r -> r instanceof PaymentResult.Replayed).count();

    assertThat(executed).as("exactly one thread performs the payment").isEqualTo(1);
    assertThat(replayed).as("every other thread sees a replay").isEqualTo(THREADS - 1);
    assertThat(payments.size()).as("one journal entry for one logical payment").isEqualTo(1);

    // The balance is deliberately NOT asserted here, and that omission is the point.
    //
    // The service debits before appending to the journal, so the threads that go on to lose the
    // unique-key race have already moved money. In production the debit and the append share one
    // transaction, so losing the race rolls the debit back and the balance ends up reduced
    // exactly once. The in-memory repositories have no transaction, so here the account really is
    // debited once per thread while only one entry is journalled -- money leaves with no record.
    //
    // That is not a defect in the service; it is the limit of what a non-transactional test
    // double can model, and asserting the production invariant against it would require the
    // double to grow a unit of work it has no business having. The assertion that the balance
    // falls by exactly one payment belongs with a real database, and is made in the stage 2
    // suite against PostgreSQL. Treating this test as evidence that double spending is prevented
    // would be a mistake: it shows the key guard admits exactly one writer, nothing more.
  }

  @Test
  @DisplayName("payments on different accounts do not interfere")
  void isolatesDistinctAccounts() throws Exception {
    List<AccountId> ids = new ArrayList<>();
    for (int i = 0; i < THREADS; i++) {
      AccountId id = new AccountId(UUID.randomUUID());
      ids.add(id);
      accounts.save(new Account(id, OWNER, Money.of("100.00", "CHF"), NOW, NOW));
    }

    List<PaymentResult> results =
        runConcurrently(
            i ->
                () ->
                    service.submit(
                        new PaymentRequest(
                            OWNER,
                            ids.get(i),
                            new IdempotencyKey("key-" + i),
                            Money.of("100.00", "CHF"),
                            BENEFICIARY,
                            null)));

    // Serialisation is per account, so independent accounts all succeed. Were the implementation
    // to take a global lock this would still pass -- but it documents the intended isolation, and
    // the stage 2 suite measures that different accounts genuinely proceed in parallel.
    assertThat(results).allMatch(r -> r instanceof PaymentResult.Completed);
    assertThat(ids)
        .allSatisfy(
            id -> assertThat(accounts.findById(id).orElseThrow().balance().isZero()).isTrue());
  }

  private PaymentRequest requestFor(String amount, String idempotencyKey) {
    return new PaymentRequest(
        OWNER,
        ACCOUNT,
        new IdempotencyKey(idempotencyKey),
        Money.of(amount, "CHF"),
        BENEFICIARY,
        null);
  }

  /**
   * Runs one task per thread, released simultaneously from a barrier.
   *
   * <p>The barrier matters: without it, threads started in a loop tend to run almost sequentially,
   * and the test would pass whether or not the code is safe. Releasing them together maximises the
   * chance of genuine interleaving.
   */
  private List<PaymentResult> runConcurrently(TaskFactory factory) throws Exception {
    CyclicBarrier startLine = new CyclicBarrier(THREADS);
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      List<Future<PaymentResult>> futures = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        Callable<PaymentResult> task = factory.create(i);
        futures.add(
            pool.submit(
                () -> {
                  startLine.await(10, TimeUnit.SECONDS);
                  return task.call();
                }));
      }

      List<PaymentResult> results = new ArrayList<>();
      for (Future<PaymentResult> future : futures) {
        results.add(future.get(10, TimeUnit.SECONDS));
      }
      return results;
    } finally {
      pool.shutdownNow();
    }
  }

  @FunctionalInterface
  private interface TaskFactory {
    Callable<PaymentResult> create(int index);
  }
}
