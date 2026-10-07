package com.alpian.payment.config;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.SQLExceptionSubclassTranslator;

/**
 * Supplies the {@link JdbcClient} used by the repositories, with lock-acquisition failures
 * translated into the exception type the service layer acts on.
 */
@Configuration
public class JdbcConfiguration {

  /** PostgreSQL {@code lock_not_available}: a {@code lock_timeout} elapsed. */
  private static final String LOCK_NOT_AVAILABLE = "55P03";

  /** PostgreSQL {@code deadlock_detected}. */
  private static final String DEADLOCK_DETECTED = "40P01";

  /** Overrides Spring Boot's auto-configured client, which is {@code @ConditionalOnMissingBean}. */
  @Bean
  public JdbcClient jdbcClient(DataSource dataSource) {
    return create(dataSource);
  }

  /**
   * Builds the client.
   *
   * <p>Exposed as a static factory so integration tests construct the client exactly as the
   * application does. Were a test to use a plain {@code JdbcClient.create(dataSource)}, it would
   * observe different exception types from production and could pass while the real mapping was
   * broken — which is precisely the defect this class exists to fix.
   */
  public static JdbcClient create(DataSource dataSource) {
    JdbcTemplate template = new JdbcTemplate(dataSource);
    template.setExceptionTranslator(new LockAwareExceptionTranslator());
    return JdbcClient.create(template);
  }

  /**
   * Maps PostgreSQL's lock-failure states onto {@link CannotAcquireLockException}.
   *
   * <p>Spring's default chain does not. The driver raises a plain {@code PSQLException} rather than
   * one of the JDBC 4 subclasses, so subclass translation does not apply, and the fallback
   * state-based translator does not recognise SQLSTATE class {@code 55}. A {@code lock_timeout}
   * therefore arrives as {@code UncategorizedSQLException} — indistinguishable from any other
   * database fault.
   *
   * <p>That matters because lock contention is not a fault: nothing is wrong with the request, it
   * merely arrived while another payment on the same account was in flight, and the correct answer
   * is a retryable rejection. Left uncategorised it would become an opaque 500, and clients would
   * have no way to tell "retry this" from "this will never work".
   *
   * <p>Translating centrally rather than inspecting SQLSTATE at each call site means the semantic
   * type is correct everywhere, including in code written later by someone who does not know this
   * quirk exists.
   */
  static class LockAwareExceptionTranslator extends SQLExceptionSubclassTranslator {

    @Override
    protected DataAccessException doTranslate(String task, String sql, SQLException ex) {
      String state = ex.getSQLState();
      if (LOCK_NOT_AVAILABLE.equals(state) || DEADLOCK_DETECTED.equals(state)) {
        return new CannotAcquireLockException(buildMessage(task, sql, ex), ex);
      }
      return super.doTranslate(task, sql, ex);
    }
  }
}
