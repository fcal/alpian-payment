package com.alpian.payment.repository;

import com.alpian.payment.domain.Account;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class AccountRepository {

  private static final String LOCK_NOT_AVAILABLE = "55P03";

  private static final String SELECT =
      "SELECT id, user_id, balance, currency, updated_at FROM account WHERE id = :id";

  private final JdbcClient jdbc;
  private final long lockTimeoutMillis;

  public AccountRepository(
      JdbcClient jdbc, @Value("${payment.lock-timeout:3s}") Duration lockTimeout) {
    this.jdbc = jdbc;
    this.lockTimeoutMillis = lockTimeout.toMillis();
  }

  public Optional<Account> find(UUID id) {
    return jdbc.sql(SELECT).param("id", id).query(AccountRepository::map).optional();
  }

  /**
   * Locks the account row until the end of the current transaction, which serialises payments on
   * one account. Waits at most {@code payment.lock-timeout}, then throws {@code
   * CannotAcquireLockException}: a bounded wait keeps one hot account from exhausting the pool.
   */
  public Optional<Account> lock(UUID id) {
    // SET LOCAL scopes the timeout to this transaction. SET takes no bind parameters.
    jdbc.sql("SET LOCAL lock_timeout = " + lockTimeoutMillis).update();
    try {
      return jdbc.sql(SELECT + " FOR UPDATE")
          .param("id", id)
          .query(AccountRepository::map)
          .optional();
    } catch (UncategorizedSQLException e) {
      // Spring leaves Postgres' lock_not_available uncategorised; translate it here.
      if (LOCK_NOT_AVAILABLE.equals(e.getSQLException().getSQLState())) {
        throw new CannotAcquireLockException("Account " + id + " is locked", e);
      }
      throw e;
    }
  }

  /** Call with the row locked; the CHECK (balance >= 0) constraint backs this up. */
  public void debit(UUID id, BigDecimal amount) {
    jdbc.sql("UPDATE account SET balance = balance - :amount, updated_at = now() WHERE id = :id")
        .param("amount", amount)
        .param("id", id)
        .update();
  }

  private static Account map(ResultSet rs, int row) throws SQLException {
    return new Account(
        rs.getObject("id", UUID.class),
        rs.getObject("user_id", UUID.class),
        rs.getBigDecimal("balance"),
        rs.getString("currency"),
        rs.getTimestamp("updated_at").toInstant());
  }
}
