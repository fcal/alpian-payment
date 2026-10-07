package com.alpian.payment.repository.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;

/**
 * Pins down how a {@code lock_timeout} surfaces through Spring's exception translation.
 *
 * <p>Its own test because {@link com.alpian.payment.service.PaymentService} catches a specific
 * exception type to produce the {@code LOCK_TIMEOUT} outcome, and that mapping was asserted from
 * documentation rather than observation. If the translation ever differs — a driver change, a
 * different translator — the catch block stops matching and lock contention degrades from a clean
 * retryable rejection into an opaque 500. That regression would be invisible without this test.
 */
class LockTimeoutTranslationTest extends PostgresTestBase {

  @Test
  @DisplayName("a lock_timeout surfaces as CannotAcquireLockException with SQLSTATE 55P03")
  void translatesLockTimeout() throws Exception {
    AccountId id = new AccountId(UUID.randomUUID());
    UserId owner = new UserId(UUID.randomUUID());
    seedUser(owner);
    accounts.save(new Account(id, owner, Money.of("100.00", "CHF"), Instant.now(), Instant.now()));

    // Hold the row lock on a separate connection, outside Spring, so the repository genuinely has
    // to wait and then time out.
    try (Connection holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (var st = holder.prepareStatement("SELECT 1 FROM account WHERE id = ? FOR UPDATE")) {
        st.setObject(1, id.value());
        st.execute();
      }

      Throwable thrown =
          org.assertj.core.api.Assertions.catchThrowable(
              () -> transactions.execute(status -> accounts.debit(id, Money.of("10.00", "CHF"))));

      assertThat(thrown)
          .as("the type PaymentService catches to report LOCK_TIMEOUT")
          .isInstanceOf(CannotAcquireLockException.class);

      // 55P03 is lock_not_available. Asserted explicitly so the test documents what the database
      // reported, not merely which Spring type happened to wrap it. Read off SQLException rather
      // than the driver's own subclass, so the test does not depend on the driver being on the
      // compile classpath.
      assertThat(sqlStateOf(thrown)).isEqualTo("55P03");

      holder.rollback();
    }
  }

  @Test
  @DisplayName("the balance is untouched when the lock cannot be acquired")
  void leavesBalanceUnchangedOnTimeout() throws Exception {
    AccountId id = new AccountId(UUID.randomUUID());
    UserId owner = new UserId(UUID.randomUUID());
    seedUser(owner);
    accounts.save(new Account(id, owner, Money.of("100.00", "CHF"), Instant.now(), Instant.now()));

    try (Connection holder = dataSource.getConnection()) {
      holder.setAutoCommit(false);
      try (var st = holder.prepareStatement("SELECT 1 FROM account WHERE id = ? FOR UPDATE")) {
        st.setObject(1, id.value());
        st.execute();
      }

      assertThatThrownBy(
              () -> transactions.execute(status -> accounts.debit(id, Money.of("10.00", "CHF"))))
          .isInstanceOf(CannotAcquireLockException.class);

      holder.rollback();
    }

    assertThat(accounts.findById(id).orElseThrow().balance()).isEqualTo(Money.of("100.00", "CHF"));
  }

  @Test
  @DisplayName("debit refuses to run outside a transaction, where FOR UPDATE would be useless")
  void refusesToDebitWithoutATransaction() {
    AccountId id = new AccountId(UUID.randomUUID());
    UserId owner = new UserId(UUID.randomUUID());
    seedUser(owner);
    accounts.save(new Account(id, owner, Money.of("100.00", "CHF"), Instant.now(), Instant.now()));

    // The guard exists because this failure is otherwise silent: in autocommit the SELECT ... FOR
    // UPDATE succeeds and releases the lock immediately, so the code appears to work while
    // providing no protection at all against concurrent debits.
    assertThatThrownBy(() -> accounts.debit(id, Money.of("10.00", "CHF")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must run inside a transaction");

    assertThat(accounts.findById(id).orElseThrow().balance()).isEqualTo(Money.of("100.00", "CHF"));
  }

  private String sqlStateOf(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof SQLException sql && sql.getSQLState() != null) {
        return sql.getSQLState();
      }
    }
    throw new AssertionError("No SQLException with a SQLSTATE in the cause chain of " + thrown);
  }

  private void seedUser(UserId id) {
    jdbc.sql("INSERT INTO app_user (id, name) VALUES (:id, :name)")
        .param("id", id.value())
        .param("name", "Test User")
        .update();
  }
}
