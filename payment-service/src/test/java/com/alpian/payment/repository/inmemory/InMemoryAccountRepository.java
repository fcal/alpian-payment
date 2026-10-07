package com.alpian.payment.repository.inmemory;

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.repository.AccountRepository;
import com.alpian.payment.repository.DebitResult;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory {@link AccountRepository} for unit tests.
 *
 * <p>Lives in the test source set on purpose: it is a test double, and shipping it in the
 * production artefact would be dead code inviting accidental use. If a database-less development
 * mode were ever wanted it would move to {@code main} behind a profile.
 *
 * <p>{@link #debit} is genuinely atomic per account, via {@link ConcurrentHashMap#compute}, which
 * holds the bin lock for that key. So it does honour the interface contract under concurrency
 * <em>within one JVM</em> — enough to verify that {@link com.alpian.payment.service.PaymentService}
 * is correct when given a conforming repository.
 *
 * <p>What it cannot reproduce is equally important: there are no transactions, so a failure after a
 * debit does not roll it back, and there is no coordination across processes. Those properties are
 * the ones that actually matter in production, and they are verified against real PostgreSQL in the
 * stage 2 integration tests. This class must not be taken as evidence that the locking strategy
 * works.
 */
public class InMemoryAccountRepository implements AccountRepository {

  private final Map<AccountId, Account> accounts = new ConcurrentHashMap<>();
  private final Clock clock;

  public InMemoryAccountRepository(Clock clock) {
    this.clock = clock;
  }

  @Override
  public Optional<Account> findById(AccountId id) {
    return Optional.ofNullable(accounts.get(id));
  }

  @Override
  public DebitResult debit(AccountId id, Money amount) {
    // compute() applies the whole function while holding the lock for this key, so the balance
    // check and the decrement are indivisible with respect to other callers -- the same property
    // SELECT ... FOR UPDATE provides in the PostgreSQL implementation, for a single JVM.
    AtomicReference<DebitResult> result = new AtomicReference<>();

    accounts.compute(
        id,
        (key, existing) -> {
          if (existing == null) {
            result.set(new DebitResult.AccountNotFound());
            return null;
          }
          if (!existing.balance().hasSameCurrencyAs(amount)) {
            result.set(new DebitResult.CurrencyMismatch(existing.balance()));
            return existing;
          }
          if (!existing.canCover(amount)) {
            result.set(new DebitResult.InsufficientFunds(existing.balance()));
            return existing;
          }
          Money remaining = existing.balance().subtract(amount);
          result.set(new DebitResult.Applied(remaining));
          return new Account(
              existing.id(), existing.userId(), remaining, existing.createdAt(), clock.instant());
        });

    return result.get();
  }

  @Override
  public Account save(Account account) {
    accounts.put(account.id(), account);
    return account;
  }

  @Override
  public List<Account> findByUserId(UserId userId) {
    List<Account> owned = new ArrayList<>();
    accounts.values().stream().filter(a -> a.isOwnedBy(userId)).forEach(owned::add);
    return List.copyOf(owned);
  }
}
