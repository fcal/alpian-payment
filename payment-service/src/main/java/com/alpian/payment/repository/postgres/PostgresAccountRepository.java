package com.alpian.payment.repository.postgres;

import com.alpian.payment.config.PaymentProperties;
import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.DebitResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * PostgreSQL {@link AccountRepository}, using hand-written SQL via {@link JdbcClient}.
 *
 * <p>SQL rather than an ORM because the locking is the substance of this class. {@code SELECT ...
 * FOR UPDATE} followed by a conditional decrement reads plainly here, whereas JPA's natural idiom —
 * load the entity, mutate it, let the flush write it — is precisely the read-modify-write race
 * being avoided, and expressing the lock through it means fighting the abstraction.
 */
@Repository
public class PostgresAccountRepository implements AccountRepository {

  private static final Logger log = LoggerFactory.getLogger(PostgresAccountRepository.class);

  /**
   * Takes an exclusive lock on the account row for the remainder of the transaction. Concurrent
   * payments on the same account queue here; payments on other accounts are unaffected, because the
   * lock is on one row rather than the table.
   */
  private static final String LOCK_ACCOUNT =
      """
      SELECT id, user_id, balance, currency, created_at, last_updated_at
        FROM account
       WHERE id = :id
         FOR UPDATE
      """;

  /**
   * The balance is decremented relative to its current value rather than assigned a value computed
   * in Java. Under the row lock either form is safe, but a relative decrement cannot clobber a
   * concurrent change even if the lock were somehow absent, and it keeps the arithmetic in the
   * column's own numeric type.
   */
  private static final String DEBIT_ACCOUNT =
      """
      UPDATE account
         SET balance = balance - :amount,
             last_updated_at = now()
       WHERE id = :id
      """;

  private static final String FIND_BY_ID =
      """
      SELECT id, user_id, balance, currency, created_at, last_updated_at
        FROM account
       WHERE id = :id
      """;

  private static final String FIND_BY_USER_ID =
      """
      SELECT id, user_id, balance, currency, created_at, last_updated_at
        FROM account
       WHERE user_id = :userId
       ORDER BY created_at
      """;

  private static final String UPSERT_ACCOUNT =
      """
      INSERT INTO account (id, user_id, balance, currency, created_at, last_updated_at)
      VALUES (:id, :userId, :balance, :currency, :createdAt, :lastUpdatedAt)
      ON CONFLICT (id) DO UPDATE
         SET balance = EXCLUDED.balance,
             last_updated_at = EXCLUDED.last_updated_at
      """;

  private final JdbcClient jdbc;
  private final long lockTimeoutMillis;

  public PostgresAccountRepository(JdbcClient jdbc, PaymentProperties properties) {
    this.jdbc = jdbc;
    this.lockTimeoutMillis = properties.lockTimeout().toMillis();
  }

  @Override
  public DebitResult debit(AccountId id, Money amount) {
    requireTransaction();
    applyLockTimeout();

    Optional<Account> locked =
        jdbc.sql(LOCK_ACCOUNT)
            .param("id", id.value())
            .query(PostgresAccountRepository::mapAccount)
            .optional();

    if (locked.isEmpty()) {
      return new DebitResult.AccountNotFound();
    }

    Account account = locked.get();
    if (!account.balance().hasSameCurrencyAs(amount)) {
      return new DebitResult.CurrencyMismatch(account.balance());
    }
    if (!account.canCover(amount)) {
      return new DebitResult.InsufficientFunds(account.balance());
    }

    int updated =
        jdbc.sql(DEBIT_ACCOUNT).param("amount", amount.amount()).param("id", id.value()).update();

    if (updated != 1) {
      // Unreachable while the lock is held, so this indicates the lock was not in force.
      throw new IllegalStateException(
          "Expected to debit exactly one account row, updated " + updated);
    }

    return new DebitResult.Applied(account.balance().subtract(amount));
  }

  /**
   * Fails fast when called outside a transaction.
   *
   * <p>Without this the class has a silent failure mode that is close to undetectable. In
   * autocommit each statement is its own transaction, so {@code FOR UPDATE} acquires the row lock
   * and releases it the instant the select returns — the query succeeds, the code looks right, and
   * the serialisation that prevents double spending simply is not there. A loud failure is much
   * better than a quiet loss of the central guarantee.
   */
  private void requireTransaction() {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "debit() must run inside a transaction: outside one, FOR UPDATE releases the row lock "
              + "immediately and provides no protection against concurrent debits");
    }
  }

  /**
   * Bounds the wait for the row lock, for this transaction only.
   *
   * <p>{@code SET LOCAL} rather than a session-level setting or a datasource property, because a
   * session-scoped value does not survive connection pooling in transaction mode: PgBouncer hands
   * the backend to another client between transactions, so anything set at session scope silently
   * stops applying (§16.2 of the design review). Setting it per transaction is correct with or
   * without a pooler in front.
   *
   * <p>Interpolated rather than bound, since {@code SET} does not accept parameters. The value is a
   * {@code long} derived from a {@link java.time.Duration}, so there is nothing injectable about
   * it.
   */
  private void applyLockTimeout() {
    jdbc.sql("SET LOCAL lock_timeout = " + lockTimeoutMillis).update();
  }

  @Override
  public Optional<Account> findById(AccountId id) {
    return jdbc.sql(FIND_BY_ID)
        .param("id", id.value())
        .query(PostgresAccountRepository::mapAccount)
        .optional();
  }

  @Override
  public List<Account> findByUserId(UserId userId) {
    return jdbc.sql(FIND_BY_USER_ID)
        .param("userId", userId.value())
        .query(PostgresAccountRepository::mapAccount)
        .list();
  }

  @Override
  public Account save(Account account) {
    jdbc.sql(UPSERT_ACCOUNT)
        .param("id", account.id().value())
        .param("userId", account.userId().value())
        .param("balance", account.balance().amount())
        .param("currency", account.balance().currencyCode())
        .param("createdAt", java.sql.Timestamp.from(account.createdAt()))
        .param("lastUpdatedAt", java.sql.Timestamp.from(account.lastUpdatedAt()))
        .update();
    log.debug("Saved account {}", account.id());
    return account;
  }

  private static Account mapAccount(ResultSet rs, int rowNum) throws SQLException {
    // CHAR(3) is blank-padded by PostgreSQL, so trim before handing it to Currency.
    Currency currency = Currency.getInstance(rs.getString("currency").trim());
    return new Account(
        new AccountId(rs.getObject("id", java.util.UUID.class)),
        new UserId(rs.getObject("user_id", java.util.UUID.class)),
        new Money(rs.getBigDecimal("balance"), currency),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("last_updated_at").toInstant());
  }
}
