package com.alpian.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.repository.AccountRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the payment path as the Spring container actually assembles it.
 *
 * <p>The repository integration tests build the transactional proxy by hand, so they cannot catch a
 * wiring mistake — {@code PaymentExecutor} not being proxied, say, or the self-invocation trap
 * reappearing. Had that happened here, every payment would fail at the repository's {@code
 * requireTransaction} guard. This test is the evidence that it does not.
 *
 * <p>Runs against the seeded demo data from the V3 migration.
 */
@SpringBootTest(properties = "spring.docker.compose.enabled=false")
@Testcontainers
class PaymentServiceContextTest {

  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  private static final UserId ADA = UserId.of("11111111-1111-1111-1111-111111111111");
  private static final AccountId ADA_CHF = AccountId.of("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");

  @Autowired PaymentService service;
  @Autowired PaymentExecutor executor;
  @Autowired AccountRepository accounts;

  @Test
  @DisplayName("the container proxies PaymentExecutor, so its transaction boundary is real")
  void executorIsTransactionallyProxied() {
    assertThat(AopUtils.isAopProxy(executor)).isTrue();
  }

  @Test
  @DisplayName("a payment succeeds end to end through the assembled context")
  void executesAPaymentThroughTheRealWiring() {
    PaymentResult result =
        service.submit(
            new PaymentRequest(
                ADA,
                ADA_CHF,
                new IdempotencyKey("context-test"),
                Money.of("25.00", "CHF"),
                new Beneficiary("Acme GmbH", "CH9300762011623852957"),
                "context smoke test"));

    assertThat(result).isInstanceOf(PaymentResult.Completed.class);
    assertThat(accounts.findById(ADA_CHF).orElseThrow().balance())
        .isEqualTo(Money.of("975.00", "CHF"));
  }
}
