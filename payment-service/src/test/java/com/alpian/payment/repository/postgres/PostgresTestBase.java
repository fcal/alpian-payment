package com.alpian.payment.repository.postgres;

import com.alpian.payment.config.JdbcConfiguration;
import com.alpian.payment.config.PaymentProperties;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
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
    jdbcFor(ds).sql("TRUNCATE payment, account, app_user CASCADE").update();
  }

  private JdbcClient jdbcFor(DataSource ds) {
    return JdbcConfiguration.create(ds);
  }
}
