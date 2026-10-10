package com.alpian.payment.repository;

import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** The append-only payment journal. */
@Repository
public class PaymentRepository {

  private static final String SELECT =
      """
      SELECT id, account_id, idempotency_key, amount, currency, beneficiary_name,
             beneficiary_iban, reference, status, failure_reason, created_at
        FROM payment
      """;

  private final JdbcClient jdbc;

  public PaymentRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public void insert(Payment payment) {
    jdbc.sql(
            """
            INSERT INTO payment (id, account_id, idempotency_key, amount, currency,
                                 beneficiary_name, beneficiary_iban, reference,
                                 status, failure_reason, created_at)
            VALUES (:id, :accountId, :key, :amount, :currency, :name, :iban, :reference,
                    :status, :failureReason, :createdAt)
            """)
        .param("id", payment.id())
        .param("accountId", payment.accountId())
        .param("key", payment.idempotencyKey())
        .param("amount", payment.amount())
        .param("currency", payment.currency())
        .param("name", payment.beneficiaryName())
        .param("iban", payment.beneficiaryIban())
        .param("reference", payment.reference())
        .param("status", payment.status().name())
        .param("failureReason", payment.failureReason())
        .param("createdAt", Timestamp.from(payment.createdAt()))
        .update();
  }

  /** Scoped by account, so a payment is never read through an account it was not made from. */
  public Optional<Payment> find(UUID accountId, UUID paymentId) {
    return jdbc.sql(SELECT + " WHERE account_id = :accountId AND id = :id")
        .param("accountId", accountId)
        .param("id", paymentId)
        .query(PaymentRepository::map)
        .optional();
  }

  public Optional<Payment> findByIdempotencyKey(UUID accountId, String key) {
    return jdbc.sql(SELECT + " WHERE account_id = :accountId AND idempotency_key = :key")
        .param("accountId", accountId)
        .param("key", key)
        .query(PaymentRepository::map)
        .optional();
  }

  private static Payment map(ResultSet rs, int row) throws SQLException {
    return new Payment(
        rs.getObject("id", UUID.class),
        rs.getObject("account_id", UUID.class),
        rs.getString("idempotency_key"),
        rs.getBigDecimal("amount"),
        rs.getString("currency"),
        rs.getString("beneficiary_name"),
        rs.getString("beneficiary_iban"),
        rs.getString("reference"),
        PaymentStatus.valueOf(rs.getString("status")),
        rs.getString("failure_reason"),
        rs.getTimestamp("created_at").toInstant());
  }
}
