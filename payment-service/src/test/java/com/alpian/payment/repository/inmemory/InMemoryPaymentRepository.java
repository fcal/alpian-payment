package com.alpian.payment.repository.inmemory;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.repository.DuplicateIdempotencyKeyException;
import com.alpian.payment.repository.PaymentRepository;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link PaymentRepository} for unit tests. See {@link InMemoryAccountRepository} for
 * what a test double of this kind can and cannot establish.
 *
 * <p>The uniqueness of {@code (accountId, idempotencyKey)} is enforced with an atomic {@code
 * putIfAbsent} on an index, standing in for the database's unique constraint — so the
 * double-spend-on-retry guard is exercised by these tests rather than deferred entirely to
 * integration.
 */
public class InMemoryPaymentRepository implements PaymentRepository {

  private record IdempotencyIndexKey(AccountId accountId, IdempotencyKey idempotencyKey) {}

  private final Map<PaymentId, Payment> byId = new ConcurrentHashMap<>();
  private final Map<IdempotencyIndexKey, PaymentId> byIdempotencyKey = new ConcurrentHashMap<>();

  @Override
  public Payment append(Payment payment) {
    IdempotencyIndexKey index =
        new IdempotencyIndexKey(payment.accountId(), payment.idempotencyKey());

    // Claim the key before publishing the row, so a concurrent caller either wins the claim or
    // sees the constraint fire -- never both succeeding.
    PaymentId existing = byIdempotencyKey.putIfAbsent(index, payment.id());
    if (existing != null) {
      throw new DuplicateIdempotencyKeyException(payment.accountId(), payment.idempotencyKey());
    }

    byId.put(payment.id(), payment);
    return payment;
  }

  @Override
  public Optional<Payment> findById(PaymentId id) {
    return Optional.ofNullable(byId.get(id));
  }

  @Override
  public Optional<Payment> findByAccountIdAndIdempotencyKey(
      AccountId accountId, IdempotencyKey key) {
    return Optional.ofNullable(byIdempotencyKey.get(new IdempotencyIndexKey(accountId, key)))
        // The index is claimed a moment before the row is published, so a concurrent reader can
        // observe the claim without the row. flatMap yields empty in that window rather than
        // throwing, which matches what a real transaction would show: nothing committed yet.
        .flatMap(this::findById);
  }

  @Override
  public List<Payment> findByAccountId(AccountId accountId) {
    return byId.values().stream()
        .filter(p -> p.accountId().equals(accountId))
        .sorted(Comparator.comparing(Payment::createdAt).reversed())
        .toList();
  }

  /** Total appended entries, for assertions about how many attempts were recorded. */
  public int size() {
    return byId.size();
  }
}
