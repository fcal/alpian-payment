package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.support.ApplicationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Verifies the payment path as the Spring container actually assembles it.
 *
 * <p>The repository integration tests build the transactional proxy by hand, so they cannot catch a
 * wiring mistake — {@code PaymentExecutor} not being proxied, say, or the self-invocation trap
 * reappearing. Had that happened, every payment would fail at the repository's {@code
 * requireTransaction} guard. This test is the evidence that it does not.
 */
class PaymentServiceContextTest extends ApplicationTestBase {

  @Autowired PaymentService service;
  @Autowired PaymentExecutor executor;

  @Test
  @DisplayName("the container proxies PaymentExecutor, so its transaction boundary is real")
  void executorIsTransactionallyProxied() {
    assertThat(AopUtils.isAopProxy(executor)).isTrue();
  }

  @Test
  @DisplayName("a payment succeeds end to end through the assembled context")
  void executesAPaymentThroughTheRealWiring() {
    Fixture fixture = givenAccount("1000.00", "CHF");

    PaymentResult result =
        service.submit(
            new PaymentRequest(
                fixture.user(),
                fixture.account(),
                new IdempotencyKey("context-test"),
                Money.of("25.00", "CHF"),
                new Beneficiary("Acme GmbH", "CH9300762011623852957"),
                "context smoke test"));

    assertThat(result).isInstanceOf(PaymentResult.Completed.class);
    assertThat(accounts.findById(fixture.account()).orElseThrow().balance())
        .isEqualTo(Money.of("975.00", "CHF"));
  }
}
