package com.alpian.payment.repository.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.service.PaymentRequest;
import com.alpian.payment.service.PaymentService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
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
 * The authoritative concurrency suite: real PostgreSQL, real transactions, real row locks.
 *
 * <p>The equivalent Stage 1 tests run against in-memory doubles and can only show that the service
 * adds no race of its own. These show that the mechanism actually prevents double spending, which
 * is the central claim of the whole exercise.
 *
 * <p>The service comes from {@link PostgresTestBase#paymentService}, which supplies the genuine
 * transaction boundary these guarantees depend on.
 */
class PaymentConcurrencyIntegrationTest extends PostgresTestBase {

  private static final int THREADS = 32;
  private static final Beneficiary BENEFICIARY =
      new Beneficiary("Acme GmbH", "CH9300762011623852957");

  private UserId owner;
  private AccountId account;
  private PaymentService service;

  @BeforeEach
  void setUpService() {
    owner = new UserId(UUID.randomUUID());
    account = new AccountId(UUID.randomUUID());
    seedUser(owner);

    service = paymentService(Clock.systemUTC(), new SimpleMeterRegistry());
  }

  @Test
  @DisplayName("32 concurrent payments on one account never overdraw it")
  void neverOverdrawsUnderRealConcurrency() throws Exception {
    givenBalance("1000.00");

    List<PaymentResult> results =
        runConcurrently(i -> () -> service.submit(request("100.00", "key-" + i)));

    long completed = count(results, PaymentResult.Completed.class);
    long declined = count(results, PaymentResult.Declined.class);

    assertThat(completed).as("only ten payments of 100 fit in a balance of 1000").isEqualTo(10);
    assertThat(declined).isEqualTo(THREADS - 10);
    assertThat(balance()).isEqualTo(Money.of("0.00", "CHF"));
    assertThat(payments.findByAccountId(account)).as("every attempt journalled").hasSize(THREADS);
    assertThat(outboxRows()).as("one event per journalled attempt").isEqualTo(THREADS);
  }

  @Test
  @DisplayName("the balance always reconciles exactly against the journal")
  void balanceReconcilesWithTheJournal() throws Exception {
    givenBalance("500.00");

    runConcurrently(i -> () -> service.submit(request("30.00", "key-" + i)));

    Money debited =
        payments.findByAccountId(account).stream()
            .filter(Payment::isCompleted)
            .map(Payment::amount)
            .reduce(Money.of("0.00", "CHF"), Money::add);

    // The strongest available statement: whatever the interleaving, the money missing from the
    // account equals the money the journal says left it. No silent loss, no phantom payment.
    assertThat(balance()).isEqualTo(Money.of("500.00", "CHF").subtract(debited));
  }

  @Test
  @DisplayName("concurrent requests sharing an idempotency key debit the balance exactly once")
  void concurrentRetriesOfOneKeyDebitExactlyOnce() throws Exception {
    // THE test this whole design exists to pass, and the assertion Stage 1 could not make.
    //
    // 32 threads submit the same logical payment at once, as a retrying client would. Every one
    // passes the replay check before any has written, so correctness rests entirely on the unique
    // constraint and on the losing transactions rolling their debits back. Without the rollback
    // the account would be debited 32 times with one journal entry -- money gone, unrecorded --
    // which is exactly what the in-memory doubles do, and why this belongs here.
    givenBalance("1000.00");
    IdempotencyKey sharedKey = new IdempotencyKey("retried-key");

    List<PaymentResult> results =
        runConcurrently(
            i ->
                () ->
                    service.submit(
                        new PaymentRequest(
                            owner,
                            account,
                            sharedKey,
                            Money.of("100.00", "CHF"),
                            BENEFICIARY,
                            null)));

    long executed = count(results, PaymentResult.Completed.class);
    long replayed = count(results, PaymentResult.Replayed.class);

    assertThat(executed).as("exactly one thread performs the payment").isEqualTo(1);
    assertThat(replayed)
        .as("every other thread is told what already happened")
        .isEqualTo(THREADS - 1);
    assertThat(payments.findByAccountId(account)).as("one journal entry").hasSize(1);

    // Debited once, not 32 times. Every losing transaction's debit was rolled back.
    assertThat(balance())
        .as("1000 less exactly one payment of 100")
        .isEqualTo(Money.of("900.00", "CHF"));

    // And exactly one event. The losers' outbox rows were in the same transactions as their
    // debits, so they rolled back together: a notification for a payment that did not happen is
    // as impossible as the payment itself.
    assertThat(outboxRows()).as("one event for one payment").isEqualTo(1);
  }

  @Test
  @DisplayName("the database refuses to overdraw even if the application tries")
  void databaseConstraintIsTheFinalGuard() {
    givenBalance("100.00");

    // Bypasses the service entirely and attempts a raw overdraft, standing in for any future bug
    // that reaches the column without checking. The CHECK constraint, not the application, is
    // what makes "balance >= 0" unconditional.
    Throwable thrown =
        org.assertj.core.api.Assertions.catchThrowable(
            () ->
                jdbc.sql("UPDATE account SET balance = balance - 500 WHERE id = :id")
                    .param("id", account.value())
                    .update());

    assertThat(thrown).isNotNull();
    assertThat(thrown.getMessage()).contains("account_balance_non_negative");
    assertThat(balance()).isEqualTo(Money.of("100.00", "CHF"));
  }

  @Test
  @DisplayName("payments on different accounts proceed in parallel, not behind one another")
  void locksPerAccountRatherThanGlobally() throws Exception {
    // Distinguishes a row lock from a table lock. A second account is debited while the first is
    // held locked by an unrelated connection: if the implementation serialised globally, this
    // would time out instead of succeeding.
    givenBalance("100.00");
    AccountId other = new AccountId(UUID.randomUUID());
    accounts.save(
        new Account(other, owner, Money.of("100.00", "CHF"), Instant.now(), Instant.now()));

    try (var holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (var st = holder.prepareStatement("SELECT 1 FROM account WHERE id = ? FOR UPDATE")) {
        st.setObject(1, account.value());
        st.execute();
      }

      PaymentResult onBlockedAccount = service.submit(request("10.00", "blocked"));
      PaymentResult onFreeAccount =
          service.submit(
              new PaymentRequest(
                  owner,
                  other,
                  new IdempotencyKey("free"),
                  Money.of("10.00", "CHF"),
                  BENEFICIARY,
                  null));

      assertThat(onBlockedAccount.outcome())
          .as("contended account yields a retryable rejection")
          .isEqualTo(com.alpian.payment.domain.PaymentOutcome.LOCK_TIMEOUT);
      assertThat(onFreeAccount)
          .as("an unrelated account is unaffected")
          .isInstanceOf(PaymentResult.Completed.class);

      holder.rollback();
    }

    assertThat(balance())
        .as("the contended payment changed nothing")
        .isEqualTo(Money.of("100.00", "CHF"));
  }

  @Test
  @DisplayName("a declined payment is journalled as FAILED with its reason")
  void journalsDeclinesWithReason() {
    givenBalance("10.00");

    PaymentResult result = service.submit(request("50.00", "key-1"));

    assertThat(result).isInstanceOf(PaymentResult.Declined.class);
    Payment stored =
        payments
            .findByIdAndAccountId(result.journalEntry().orElseThrow().id(), account)
            .orElseThrow();
    assertThat(stored.status()).isEqualTo(PaymentStatus.FAILED);
    assertThat(stored.failureReasonIfAny()).contains("Insufficient funds");
    assertThat(balance()).isEqualTo(Money.of("10.00", "CHF"));
  }

  @Test
  @DisplayName("a payment round-trips through the database unchanged")
  void persistsEveryFieldFaithfully() {
    givenBalance("1000.00");

    PaymentResult result = service.submit(request("123.45", "key-1"));
    Payment original = result.journalEntry().orElseThrow();
    Payment reloaded = payments.findByIdAndAccountId(original.id(), account).orElseThrow();

    // Equality across the whole record, which also pins down that Money survives NUMERIC(19,4)
    // and that the timestamp survives TIMESTAMPTZ.
    assertThat(reloaded).isEqualTo(original);
  }

  @Test
  @DisplayName("a payment is not found through an account it was not made from")
  void scopesPaymentLookupToItsAccount() {
    givenBalance("1000.00");
    Payment payment = service.submit(request("10.00", "key-1")).journalEntry().orElseThrow();
    AccountId other = new AccountId(UUID.randomUUID());
    accounts.save(new Account(other, owner, Money.of("0.00", "CHF"), Instant.now(), Instant.now()));

    // Even an account belonging to the same user: the payment was made from `account`.
    assertThat(payments.findByIdAndAccountId(payment.id(), other)).isEmpty();
    assertThat(payments.findByIdAndAccountId(payment.id(), account)).isPresent();
  }

  private void givenBalance(String amount) {
    accounts.save(
        new Account(account, owner, Money.of(amount, "CHF"), Instant.now(), Instant.now()));
  }

  private long outboxRows() {
    return jdbc.sql("SELECT count(*) FROM outbox").query(Long.class).single();
  }

  private Money balance() {
    return accounts.findById(account).orElseThrow().balance();
  }

  private PaymentRequest request(String amount, String key) {
    return new PaymentRequest(
        owner, account, new IdempotencyKey(key), Money.of(amount, "CHF"), BENEFICIARY, null);
  }

  private long count(List<PaymentResult> results, Class<? extends PaymentResult> type) {
    return results.stream().filter(type::isInstance).count();
  }

  private void seedUser(UserId id) {
    jdbc.sql("INSERT INTO app_user (id, name) VALUES (:id, :name)")
        .param("id", id.value())
        .param("name", "Test User")
        .update();
  }

  /** Releases all threads from a barrier so the interleaving is genuine rather than incidental. */
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
                  startLine.await(30, TimeUnit.SECONDS);
                  return task.call();
                }));
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

  @FunctionalInterface
  private interface TaskFactory {
    Callable<PaymentResult> create(int index);
  }
}
