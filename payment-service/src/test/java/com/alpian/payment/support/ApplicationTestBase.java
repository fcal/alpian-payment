package com.alpian.payment.support;

import com.alpian.payment.domain.PaymentRequest;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Boots the application against a shared PostgreSQL container. The relay is off: there is no
 * broker. Each test creates its own account, so tests do not depend on each other.
 */
@SpringBootTest(
    properties = {
      "spring.docker.compose.enabled=false",
      "payment.outbox.relay-enabled=false",
      "payment.lock-timeout=1s"
    })
public abstract class ApplicationTestBase {

  @ServiceConnection
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine");

  static {
    POSTGRES.start();
  }

  @Autowired protected JdbcClient jdbc;

  public record Fixture(UUID user, UUID account) {

    public PaymentRequest request(String amount, String key) {
      return new PaymentRequest(
          user,
          account,
          key,
          new BigDecimal(amount),
          "CHF",
          "Acme GmbH",
          "CH93 0076 2011 6238 5295 7",
          null);
    }
  }

  protected Fixture givenAccount(String balance, String currency) {
    return givenAccount(UUID.randomUUID(), balance, currency);
  }

  protected Fixture givenAccount(UUID user, String balance, String currency) {
    Fixture fixture = new Fixture(user, UUID.randomUUID());
    jdbc.sql(
            "INSERT INTO account (id, user_id, balance, currency) VALUES (:id, :user, :balance, :currency)")
        .param("id", fixture.account())
        .param("user", fixture.user())
        .param("balance", new BigDecimal(balance))
        .param("currency", currency)
        .update();
    return fixture;
  }

  protected BigDecimal balance(UUID account) {
    return jdbc.sql("SELECT balance FROM account WHERE id = :id")
        .param("id", account)
        .query(BigDecimal.class)
        .single();
  }

  protected long payments(UUID account) {
    return jdbc.sql("SELECT count(*) FROM payment WHERE account_id = :account")
        .param("account", account)
        .query(Long.class)
        .single();
  }

  protected long outboxRows(UUID account) {
    return jdbc.sql("SELECT count(*) FROM outbox WHERE partition_key = :account")
        .param("account", account.toString())
        .query(Long.class)
        .single();
  }
}
