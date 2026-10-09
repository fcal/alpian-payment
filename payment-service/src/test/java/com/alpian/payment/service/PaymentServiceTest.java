package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentRequest;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.support.ApplicationTestBase;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Business rules of {@link PaymentService}, against a real database. */
class PaymentServiceTest extends ApplicationTestBase {

  @Autowired PaymentService service;

  @Test
  @DisplayName("a covered payment debits the account, is journalled and enqueues one event")
  void completesAPayment() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    PaymentResult result = service.submit(f.request("250.50", "key-1"));

    assertThat(result.outcome()).isEqualTo(PaymentOutcome.COMPLETED);
    assertThat(result.payment().beneficiaryIban()).isEqualTo("CH9300762011623852957");
    assertThat(balance(f.account())).isEqualByComparingTo("749.50");
    assertThat(service.payment(f.user(), f.account(), result.payment().id()))
        .contains(result.payment());

    byte[] payload =
        jdbc.sql("SELECT payload FROM outbox WHERE payment_id = :id")
            .param("id", result.payment().id())
            .query(byte[].class)
            .single();
    PaymentEvent event = PaymentEvent.parseFrom(payload);
    assertThat(event.getUserId()).isEqualTo(f.user().toString());
    assertThat(event.getAmount().getAmount()).isEqualTo("250.5000");
  }

  @Test
  @DisplayName(
      "an uncovered payment is declined, journalled as FAILED and notified, funds untouched")
  void declinesInsufficientFunds() {
    Fixture f = givenAccount("100.00", "CHF");

    PaymentResult result = service.submit(f.request("100.01", "key-1"));

    assertThat(result.outcome()).isEqualTo(PaymentOutcome.INSUFFICIENT_FUNDS);
    assertThat(result.payment().status()).isEqualTo(PaymentStatus.FAILED);
    assertThat(result.payment().failureReason()).isEqualTo("Insufficient funds");
    assertThat(balance(f.account())).isEqualByComparingTo("100.00");
    assertThat(outboxRows(f.account())).isEqualTo(1);
  }

  @Test
  void permitsSpendingTheWholeBalance() {
    Fixture f = givenAccount("100.00", "CHF");

    assertThat(service.submit(f.request("100.00", "key-1")).outcome())
        .isEqualTo(PaymentOutcome.COMPLETED);
    assertThat(balance(f.account())).isZero();
  }

  @Test
  @DisplayName("a retry with the same key replays the original and does not debit again")
  void replaysARetry() {
    Fixture f = givenAccount("1000.00", "CHF");
    PaymentResult original = service.submit(f.request("250.00", "same-key"));

    // 250.0 is the same amount as 250.00: an equivalent request is still a replay.
    PaymentResult replay = service.submit(f.request("250.0", "same-key"));

    assertThat(replay.outcome()).isEqualTo(PaymentOutcome.REPLAYED);
    assertThat(replay.payment().id()).isEqualTo(original.payment().id());
    assertThat(balance(f.account())).isEqualByComparingTo("750.00");
    assertThat(payments(f.account())).isEqualTo(1);
    assertThat(outboxRows(f.account())).isEqualTo(1);
  }

  @Test
  @DisplayName("replaying the key of a declined payment returns the decline, even once funded")
  void replaysADecline() {
    Fixture f = givenAccount("10.00", "CHF");
    service.submit(f.request("50.00", "same-key"));
    jdbc.sql("UPDATE account SET balance = 1000 WHERE id = :id").param("id", f.account()).update();

    PaymentResult replay = service.submit(f.request("50.00", "same-key"));

    assertThat(replay.outcome()).isEqualTo(PaymentOutcome.REPLAYED);
    assertThat(replay.payment().status()).isEqualTo(PaymentStatus.FAILED);
    assertThat(balance(f.account())).isEqualByComparingTo("1000.00");
  }

  @Test
  void refusesAKeyReusedForADifferentPayment() {
    Fixture f = givenAccount("1000.00", "CHF");
    service.submit(f.request("250.00", "same-key"));

    PaymentResult reused = service.submit(f.request("999.00", "same-key"));

    assertThat(reused.outcome()).isEqualTo(PaymentOutcome.IDEMPOTENCY_KEY_REUSED);
    assertThat(balance(f.account())).isEqualByComparingTo("750.00");
  }

  @Test
  void rejectsAnUnknownAccount() {
    Fixture f = new Fixture(UUID.randomUUID(), UUID.randomUUID());

    assertThat(service.submit(f.request("10.00", "key-1")).outcome())
        .isEqualTo(PaymentOutcome.ACCOUNT_NOT_FOUND);
  }

  @Test
  @DisplayName("another user's account is reported as not found, even with its idempotency key")
  void rejectsAnotherUsersAccount() {
    Fixture owner = givenAccount("1000.00", "CHF");
    service.submit(owner.request("10.00", "owners-key"));
    Fixture intruder = new Fixture(UUID.randomUUID(), owner.account());

    PaymentResult result = service.submit(intruder.request("10.00", "owners-key"));

    assertThat(result.outcome()).isEqualTo(PaymentOutcome.ACCOUNT_NOT_FOUND);
    assertThat(result.payment()).isNull();
    assertThat(balance(owner.account())).isEqualByComparingTo("990.00");
    assertThat(service.account(intruder.user(), owner.account())).isEmpty();
  }

  @Test
  void rejectsACurrencyMismatch() {
    Fixture f = givenAccount("1000.00", "EUR");

    assertThat(service.submit(f.request("10.00", "key-1")).outcome())
        .isEqualTo(PaymentOutcome.CURRENCY_MISMATCH);
    assertThat(payments(f.account())).isZero();
  }

  @Test
  @DisplayName("a payment is only readable through the account it was made from")
  void scopesPaymentReadsToTheirAccount() {
    Fixture f = givenAccount("1000.00", "CHF");
    Fixture other = givenAccount(f.user(), "0.00", "CHF");
    UUID paymentId = service.submit(f.request("10.00", "key-1")).payment().id();

    assertThat(service.payment(f.user(), other.account(), paymentId)).isEmpty();
  }

  @Test
  void normalisesTheRequest() {
    PaymentRequest request =
        new PaymentRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "k",
            new BigDecimal("10.5"),
            "CHF",
            " Acme ",
            "ch93 0076",
            " ");

    assertThat(request.amount()).isEqualTo(new BigDecimal("10.5000"));
    assertThat(request.beneficiaryName()).isEqualTo("Acme");
    assertThat(request.beneficiaryIban()).isEqualTo("CH930076");
    assertThat(request.reference()).isNull();
  }
}
