package com.alpian.payment.support;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.repository.AccountRepository;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base for tests that boot the full application against a real PostgreSQL.
 *
 * <p>The container is static and started once, and every subclass shares the same Spring
 * configuration, so the application context is cached and reused across classes. Without that, each
 * test class would pay for a fresh container and a fresh context.
 *
 * <p>Because the database is shared, tests create their own user and account through {@link
 * #givenAccount} rather than relying on the seeded demo rows. Seed data mutated by one class would
 * otherwise make another's assertions depend on execution order.
 */
// The relay is off: these tests have no broker, and with it on, payments made here would be
// published to whatever Kafka listens on localhost:9092 -- on a developer machine, the dev stack.
@SpringBootTest(
    properties = {"spring.docker.compose.enabled=false", "payment.outbox.relay-enabled=false"})
public abstract class ApplicationTestBase {

  @ServiceConnection
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine");

  static {
    POSTGRES.start();
  }

  @Autowired protected AccountRepository accounts;
  @Autowired protected JdbcClient jdbc;

  /** A user who owns a freshly created account. */
  protected record Fixture(UserId user, AccountId account) {}

  protected Fixture givenAccount(String balance, String currency) {
    UserId user = new UserId(UUID.randomUUID());
    jdbc.sql("INSERT INTO app_user (id, name) VALUES (:id, 'Test User')")
        .param("id", user.value())
        .update();
    AccountId account = new AccountId(UUID.randomUUID());
    accounts.save(
        new Account(account, user, Money.of(balance, currency), Instant.now(), Instant.now()));
    return new Fixture(user, account);
  }
}
