package com.alpian.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.events.v1.PaymentEvent;
import com.alpian.payment.events.v1.PaymentStatus;
import com.alpian.payment.repository.OutboxMessage;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PaymentEventsTest {

  private static final UserId PAYER = UserId.of("11111111-1111-1111-1111-111111111111");
  private static final AccountId ACCOUNT = AccountId.of("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");
  private static final Instant AT = Instant.parse("2026-10-08T09:15:30.123456789Z");

  private static Payment completed(String amount, String reference) {
    return Payment.completed(
        PaymentId.of("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        ACCOUNT,
        new IdempotencyKey("k"),
        Money.of(amount, "CHF"),
        new Beneficiary("Acme GmbH", "CH9300762011623852957"),
        reference,
        AT);
  }

  @Test
  @DisplayName("the event round-trips through its serialised form with every field intact")
  void serialisesACompletedPayment() throws Exception {
    OutboxMessage message = PaymentEvents.toOutboxMessage(completed("250.50", "Invoice 42"), PAYER);

    // Parsed back from the bytes that actually go to Kafka, not from the builder.
    PaymentEvent event = PaymentEvent.parseFrom(message.payload());

    assertThat(event.getPaymentId()).isEqualTo("cccccccc-cccc-cccc-cccc-cccccccccccc");
    assertThat(event.getAccountId()).isEqualTo(ACCOUNT.toString());
    assertThat(event.getUserId()).isEqualTo(PAYER.toString());
    assertThat(event.getStatus()).isEqualTo(PaymentStatus.PAYMENT_STATUS_COMPLETED);
    assertThat(event.getBeneficiaryName()).isEqualTo("Acme GmbH");
    assertThat(event.getReference()).isEqualTo("Invoice 42");
    assertThat(event.getFailureReason()).isEmpty();
    assertThat(event.getAmount().getCurrency()).isEqualTo("CHF");
    // Nanosecond precision survives: the event says when the payment happened, exactly.
    assertThat(
            Instant.ofEpochSecond(
                event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos()))
        .isEqualTo(AT);
  }

  @Test
  @DisplayName("the amount is an exact decimal string, never a floating point approximation")
  void carriesTheAmountExactly() throws Exception {
    // 0.1 + 0.2 territory: a value that a double cannot hold exactly.
    OutboxMessage message =
        PaymentEvents.toOutboxMessage(completed("1234567890.1234", null), PAYER);

    String amount = PaymentEvent.parseFrom(message.payload()).getAmount().getAmount();

    assertThat(new BigDecimal(amount)).isEqualByComparingTo("1234567890.1234");
    assertThat(amount).doesNotContain("E");
  }

  @Test
  void carriesTheFailureReasonOfADecline() throws Exception {
    Payment failed =
        Payment.failed(
            PaymentId.generate(),
            ACCOUNT,
            new IdempotencyKey("k"),
            Money.of("50", "CHF"),
            new Beneficiary("Acme GmbH", "CH9300762011623852957"),
            null,
            "Insufficient funds",
            AT);

    PaymentEvent event =
        PaymentEvent.parseFrom(PaymentEvents.toOutboxMessage(failed, PAYER).payload());

    assertThat(event.getStatus()).isEqualTo(PaymentStatus.PAYMENT_STATUS_FAILED);
    assertThat(event.getFailureReason()).isEqualTo("Insufficient funds");
    assertThat(event.hasOccurredAt()).isTrue();
    assertThat(event.getReference()).isEmpty();
  }

  @Test
  @DisplayName("messages are keyed by account so an account's events share a partition")
  void keysByAccount() {
    OutboxMessage message = PaymentEvents.toOutboxMessage(completed("1", null), PAYER);

    assertThat(message.partitionKey()).isEqualTo(ACCOUNT.toString());
    assertThat(message.aggregateId().toString()).isEqualTo("cccccccc-cccc-cccc-cccc-cccccccccccc");
    assertThat(message.eventType()).isEqualTo("alpian.payment.v1.PaymentEvent");
  }
}
