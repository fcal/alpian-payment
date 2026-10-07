package com.alpian.payment.repository.postgres;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.repository.DuplicateIdempotencyKeyException;
import com.alpian.payment.repository.PaymentRepository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** PostgreSQL {@link PaymentRepository}. Append-only: there is no update or delete statement. */
@Repository
public class PostgresPaymentRepository implements PaymentRepository {

  private static final String COLUMNS =
      "id, account_id, idempotency_key, amount, currency, beneficiary_name, beneficiary_iban, "
          + "reference, status, failure_reason, created_at";

  private static final String APPEND =
      """
      INSERT INTO payment (id, account_id, idempotency_key, amount, currency,
                           beneficiary_name, beneficiary_iban, reference,
                           status, failure_reason, created_at)
      VALUES (:id, :accountId, :idempotencyKey, :amount, :currency,
              :beneficiaryName, :beneficiaryIban, :reference,
              :status, :failureReason, :createdAt)
      """;

  private static final String FIND_BY_ID_AND_ACCOUNT =
      "SELECT " + COLUMNS + " FROM payment WHERE id = :id AND account_id = :accountId";

  private static final String FIND_BY_IDEMPOTENCY_KEY =
      "SELECT "
          + COLUMNS
          + " FROM payment WHERE account_id = :accountId "
          + "AND idempotency_key = :idempotencyKey";

  private static final String FIND_BY_ACCOUNT =
      "SELECT "
          + COLUMNS
          + " FROM payment WHERE account_id = :accountId "
          + "ORDER BY created_at DESC, id";

  private final JdbcClient jdbc;

  public PostgresPaymentRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public Payment append(Payment payment) {
    try {
      jdbc.sql(APPEND)
          .param("id", payment.id().value())
          .param("accountId", payment.accountId().value())
          .param("idempotencyKey", payment.idempotencyKey().value())
          .param("amount", payment.amount().amount())
          .param("currency", payment.amount().currencyCode())
          .param("beneficiaryName", payment.beneficiary().name())
          .param("beneficiaryIban", payment.beneficiary().iban())
          .param("reference", payment.reference())
          .param("status", payment.status().name())
          .param("failureReason", payment.failureReason())
          .param("createdAt", Timestamp.from(payment.createdAt()))
          .update();
      return payment;
    } catch (DuplicateKeyException e) {
      // The unique index on (account_id, idempotency_key) fired. This is the authoritative
      // double-spend guard, and losing this race is an expected concurrency outcome rather than a
      // fault -- so it is translated into a domain-specific type the caller can act on, instead
      // of a generic persistence failure it would have to inspect.
      //
      // The statement has aborted the transaction, so the caller cannot recover inside it: the
      // winner's row has to be read in a fresh transaction. PaymentService handles that outside
      // its transactional boundary.
      throw new DuplicateIdempotencyKeyException(payment.accountId(), payment.idempotencyKey());
    }
  }

  @Override
  public Optional<Payment> findByIdAndAccountId(PaymentId id, AccountId accountId) {
    return jdbc.sql(FIND_BY_ID_AND_ACCOUNT)
        .param("id", id.value())
        .param("accountId", accountId.value())
        .query(PostgresPaymentRepository::mapPayment)
        .optional();
  }

  @Override
  public Optional<Payment> findByAccountIdAndIdempotencyKey(
      AccountId accountId, IdempotencyKey key) {
    return jdbc.sql(FIND_BY_IDEMPOTENCY_KEY)
        .param("accountId", accountId.value())
        .param("idempotencyKey", key.value())
        .query(PostgresPaymentRepository::mapPayment)
        .optional();
  }

  @Override
  public List<Payment> findByAccountId(AccountId accountId) {
    return jdbc.sql(FIND_BY_ACCOUNT)
        .param("accountId", accountId.value())
        .query(PostgresPaymentRepository::mapPayment)
        .list();
  }

  private static Payment mapPayment(ResultSet rs, int rowNum) throws SQLException {
    Currency currency = Currency.getInstance(rs.getString("currency").trim());
    return new Payment(
        new PaymentId(rs.getObject("id", UUID.class)),
        new AccountId(rs.getObject("account_id", UUID.class)),
        new IdempotencyKey(rs.getString("idempotency_key")),
        new Money(rs.getBigDecimal("amount"), currency),
        new Beneficiary(rs.getString("beneficiary_name"), rs.getString("beneficiary_iban")),
        rs.getString("reference"),
        PaymentStatus.valueOf(rs.getString("status")),
        rs.getString("failure_reason"),
        rs.getTimestamp("created_at").toInstant());
  }
}
