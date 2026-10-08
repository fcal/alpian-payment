package com.alpian.payment.repository.postgres;

import com.alpian.payment.config.JdbcConfiguration;
import com.alpian.payment.config.PaymentProperties;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.service.PaymentExecutor;
import com.alpian.payment.service.PaymentService;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Shared PostgreSQL fixture for repository integration tests.
 *
 * <p>One container for the whole class hierarchy, started once and reused: container startup
 * dominates the runtime of these tests, and the schema is recreated between tests instead, which is
 * far cheaper. Declared {@code static} and started manually rather than with {@code @Container}, so
 * JUnit does not stop it between classes.
 *
 * <p>Flyway runs the real migrations rather than a test-only schema. A schema that differs from
 * production is a test that verifies the wrong thing — and here specifically, the check constraints
 * and the unique index <em>are</em> part of what is under test.
 */
@Testcontainers
public abstract class PostgresTestBase {

  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("payment")
          .withUsername("payment")
          .withPassword("payment");

  static {
    POSTGRES.start();
  }

  /**
   * Short on purpose. The lock-contention tests must wait for a real timeout, and three seconds
   * each would dominate the suite. Correctness does not depend on the value.
   */
  protected static final Duration LOCK_TIMEOUT = Duration.ofMillis(250);

  protected DataSource dataSource;
  protected JdbcClient jdbc;
  protected TransactionTemplate transactions;
  protected PostgresAccountRepository accounts;
  protected PostgresPaymentRepository payments;
  protected PostgresOutboxRepository outbox;

  @BeforeEach
  void prepareDatabase() {
    DriverManagerDataSource ds = new DriverManagerDataSource();
    ds.setUrl(POSTGRES.getJdbcUrl());
    ds.setUsername(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    ds.setDriverClassName("org.postgresql.Driver");
    this.dataSource = ds;

    migrateFromScratch(ds);

    // Built via the production factory so exception translation matches the application's.
    this.jdbc = JdbcConfiguration.create(ds);
    this.transactions = new TransactionTemplate(new DataSourceTransactionManager(ds));
    this.accounts = new PostgresAccountRepository(jdbc, new PaymentProperties(LOCK_TIMEOUT));
    this.payments = new PostgresPaymentRepository(jdbc);
    this.outbox = new PostgresOutboxRepository(jdbc);
  }

  /**
   * A {@link PaymentService} over the real repositories, with {@link PaymentExecutor} wrapped in a
   * genuine transactional proxy.
   *
   * <p>Built by hand rather than by the Spring container, so the repository suites stay fast. A
   * plain {@code new PaymentExecutor(...)} would leave {@code @Transactional} inert and the
   * repositories' transaction guards would reject every call, so the proxy is not optional.
   */
  protected PaymentService paymentService(Clock clock, MeterRegistry registry) {
    ProxyFactory factory = new ProxyFactory(new PaymentExecutor(accounts, payments, outbox, clock));
    factory.setProxyTargetClass(true);
    factory.addAdvice(
        new TransactionInterceptor(
            new DataSourceTransactionManager(dataSource),
            new MatchAlwaysTransactionAttributeSource()));
    return new PaymentService(
        (PaymentExecutor) factory.getProxy(), payments, new PaymentMetrics(registry, clock));
  }

  /** Drops and reapplies the production migrations, so each test starts from a known schema. */
  private void migrateFromScratch(DataSource ds) {
    org.flywaydb.core.Flyway.configure()
        .dataSource(ds)
        .locations("classpath:db/migration")
        .cleanDisabled(false)
        .load()
        .migrate();
    // The seed migration inserts demo rows; tests create their own fixtures and assert on counts,
    // so those rows are removed rather than worked around.
    jdbcFor(ds).sql("TRUNCATE outbox, payment, account, app_user CASCADE").update();
  }

  private JdbcClient jdbcFor(DataSource ds) {
    return JdbcConfiguration.create(ds);
  }
}
