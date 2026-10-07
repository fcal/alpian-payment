package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.repository.inmemory.InMemoryAccountRepository;
import com.alpian.payment.repository.inmemory.InMemoryPaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Business-rule tests for {@link PaymentService}, against the in-memory repositories.
 *
 * <p>These establish that the service makes the right decisions given a repository that honours its
 * contract. They deliberately do <em>not</em> establish that the PostgreSQL implementation honours
 * it — no in-memory double can show that row locking or transaction rollback works. That is the job
 * of the stage 2 integration tests.
 */
class PaymentServiceTest {

  private static final Instant NOW = Instant.parse("2026-10-06T12:00:00Z");
  private static final UserId OWNER = new UserId(UUID.randomUUID());
  private static final UserId SOMEONE_ELSE = new UserId(UUID.randomUUID());
  private static final AccountId ACCOUNT = new AccountId(UUID.randomUUID());
  private static final Beneficiary BENEFICIARY =
      new Beneficiary("Acme GmbH", "CH9300762011623852957");

  private InMemoryAccountRepository accounts;
  private InMemoryPaymentRepository payments;
  private MeterRegistry registry;
  private PaymentService service;

  @BeforeEach
  void setUp() {
    Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    accounts = new InMemoryAccountRepository(clock);
    payments = new InMemoryPaymentRepository();
    registry = new SimpleMeterRegistry();
    service = new PaymentService(accounts, payments, new PaymentMetrics(registry), clock);
  }

  private void givenAccount(String balance, String currency) {
    accounts.save(new Account(ACCOUNT, OWNER, Money.of(balance, currency), NOW, NOW));
  }

  private PaymentRequest request(String amount, String currency, String idempotencyKey) {
    return new PaymentRequest(
        OWNER,
        ACCOUNT,
        new IdempotencyKey(idempotencyKey),
        Money.of(amount, currency),
        BENEFICIARY,
        "invoice 42");
  }

  private double attempts(PaymentOutcome outcome) {
    return registry.find("payment.attempts").tag("outcome", outcome.tagValue()).counter().count();
  }

  @Nested
  class HappyPath {

    @Test
    void debitsTheAccountAndJournalsThePayment() {
      givenAccount("1000.00", "CHF");

      PaymentResult result = service.submit(request("250.50", "CHF", "key-1"));

      assertThat(result).isInstanceOf(PaymentResult.Completed.class);
      assertThat(result.outcome()).isEqualTo(PaymentOutcome.COMPLETED);

      Payment journalled = result.journalEntry().orElseThrow();
      assertThat(journalled.status()).isEqualTo(PaymentStatus.COMPLETED);
      assertThat(journalled.amount()).isEqualTo(Money.of("250.50", "CHF"));
      assertThat(journalled.beneficiary()).isEqualTo(BENEFICIARY);
      assertThat(journalled.referenceIfAny()).contains("invoice 42");
      assertThat(journalled.failureReasonIfAny()).isEmpty();
      assertThat(journalled.createdAt()).isEqualTo(NOW);

      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("749.50", "CHF"));
    }

    @Test
    @DisplayName("the whole balance may be spent, leaving exactly zero")
    void permitsSpendingTheEntireBalance() {
      givenAccount("100.00", "CHF");

      assertThat(service.submit(request("100.00", "CHF", "key-1")))
          .isInstanceOf(PaymentResult.Completed.class);
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance().isZero()).isTrue();
    }

    @Test
    void recordsAmountAndOutcomeMetrics() {
      givenAccount("1000.00", "CHF");

      service.submit(request("250.50", "CHF", "key-1"));

      assertThat(attempts(PaymentOutcome.COMPLETED)).isEqualTo(1);
      var amount = registry.find("payment.amount").tag("currency", "CHF").summary();
      assertThat(amount).isNotNull();
      assertThat(amount.totalAmount()).isEqualTo(250.50);
    }
  }

  @Nested
  class BalanceCheck {

    @Test
    @DisplayName("a payment beyond the balance is declined and journalled, with funds untouched")
    void declinesAndJournalsInsufficientFunds() {
      givenAccount("100.00", "CHF");

      PaymentResult result = service.submit(request("100.01", "CHF", "key-1"));

      assertThat(result).isInstanceOf(PaymentResult.Declined.class);
      assertThat(result.outcome()).isEqualTo(PaymentOutcome.INSUFFICIENT_FUNDS);

      Payment journalled = result.journalEntry().orElseThrow();
      assertThat(journalled.status()).isEqualTo(PaymentStatus.FAILED);
      assertThat(journalled.failureReasonIfAny()).contains("Insufficient funds");

      // The decline must not move money, not even by a rounding step.
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("100.00", "CHF"));
      assertThat(attempts(PaymentOutcome.INSUFFICIENT_FUNDS)).isEqualTo(1);
    }

    @Test
    @DisplayName("a decline is recorded in the journal, so the attempt is accounted for")
    void journalsDeclinedAttempts() {
      givenAccount("10.00", "CHF");

      service.submit(request("50.00", "CHF", "key-1"));

      assertThat(payments.findByAccountId(ACCOUNT)).hasSize(1);
    }
  }

  @Nested
  class Idempotency {

    @Test
    @DisplayName("replaying a key returns the original outcome and does not debit again")
    void replaysInsteadOfDebitingTwice() {
      givenAccount("1000.00", "CHF");
      PaymentRequest first = request("250.00", "CHF", "same-key");

      PaymentResult original = service.submit(first);
      PaymentResult replay = service.submit(request("250.00", "CHF", "same-key"));

      assertThat(replay).isInstanceOf(PaymentResult.Replayed.class);
      assertThat(replay.outcome()).isEqualTo(PaymentOutcome.IDEMPOTENT_REPLAY);
      // Same journal entry, not a new one with the same values.
      assertThat(replay.journalEntry().orElseThrow().id())
          .isEqualTo(original.journalEntry().orElseThrow().id());

      // The essential assertion: one debit, not two.
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("750.00", "CHF"));
      assertThat(payments.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("replaying the key of a declined payment returns that decline")
    void replaysDeclinesToo() {
      givenAccount("10.00", "CHF");
      service.submit(request("50.00", "CHF", "same-key"));

      // Funding the account does not revive the old key: a key identifies one attempt, and its
      // recorded outcome is the answer. A client wanting to try again presents a new key.
      accounts.save(new Account(ACCOUNT, OWNER, Money.of("1000.00", "CHF"), NOW, NOW));
      PaymentResult replay = service.submit(request("50.00", "CHF", "same-key"));

      assertThat(replay).isInstanceOf(PaymentResult.Replayed.class);
      assertThat(replay.journalEntry().orElseThrow().status()).isEqualTo(PaymentStatus.FAILED);
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("1000.00", "CHF"));
    }

    @Test
    @DisplayName("a different key on the same account is a different payment")
    void treatsDistinctKeysAsDistinctPayments() {
      givenAccount("1000.00", "CHF");

      service.submit(request("100.00", "CHF", "key-1"));
      service.submit(request("100.00", "CHF", "key-2"));

      assertThat(payments.size()).isEqualTo(2);
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("800.00", "CHF"));
    }
  }

  @Nested
  class Rejections {

    @Test
    void rejectsUnknownAccount() {
      PaymentResult result = service.submit(request("10.00", "CHF", "key-1"));

      assertThat(result).isInstanceOf(PaymentResult.Rejected.class);
      assertThat(result.outcome()).isEqualTo(PaymentOutcome.ACCOUNT_NOT_FOUND);
      assertThat(result.journalEntry()).isEmpty();
    }

    @Test
    @DisplayName("an account belonging to another user is refused, and nothing is journalled")
    void rejectsAccountOwnedByAnotherUser() {
      givenAccount("1000.00", "CHF");

      PaymentRequest fromOtherUser =
          new PaymentRequest(
              SOMEONE_ELSE,
              ACCOUNT,
              new IdempotencyKey("key-1"),
              Money.of("10.00", "CHF"),
              BENEFICIARY,
              null);

      PaymentResult result = service.submit(fromOtherUser);

      assertThat(result.outcome()).isEqualTo(PaymentOutcome.ACCOUNT_NOT_OWNED);
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("1000.00", "CHF"));
      assertThat(payments.size()).isZero();
    }

    @Test
    @DisplayName("the ownership failure is not disclosed to the caller")
    void doesNotRevealThatTheAccountExists() {
      givenAccount("1000.00", "CHF");

      PaymentResult owned = service.submit(request("10.00", "CHF", "key-1"));
      PaymentResult notOwned =
          service.submit(
              new PaymentRequest(
                  SOMEONE_ELSE,
                  ACCOUNT,
                  new IdempotencyKey("key-2"),
                  Money.of("10.00", "CHF"),
                  BENEFICIARY,
                  null));
      PaymentResult unknown =
          service.submit(
              new PaymentRequest(
                  OWNER,
                  new AccountId(UUID.randomUUID()),
                  new IdempotencyKey("key-3"),
                  Money.of("10.00", "CHF"),
                  BENEFICIARY,
                  null));

      assertThat(owned).isInstanceOf(PaymentResult.Completed.class);
      // Distinguishable in metrics, indistinguishable to the client: telling an attacker that an
      // account exists but is not theirs is itself a disclosure.
      assertThat(((PaymentResult.Rejected) notOwned).detail())
          .isEqualTo(((PaymentResult.Rejected) unknown).detail());
      assertThat(attempts(PaymentOutcome.ACCOUNT_NOT_OWNED)).isEqualTo(1);
      assertThat(attempts(PaymentOutcome.ACCOUNT_NOT_FOUND)).isEqualTo(1);
    }

    @Test
    @DisplayName("a currency other than the account's is refused, with no conversion attempted")
    void rejectsCurrencyMismatch() {
      givenAccount("1000.00", "CHF");

      PaymentResult result = service.submit(request("10.00", "EUR", "key-1"));

      assertThat(result.outcome()).isEqualTo(PaymentOutcome.CURRENCY_MISMATCH);
      assertThat(((PaymentResult.Rejected) result).detail()).contains("EUR").contains("CHF");
      assertThat(accounts.findById(ACCOUNT).orElseThrow().balance())
          .isEqualTo(Money.of("1000.00", "CHF"));
      assertThat(payments.size()).isZero();
    }
  }
}
