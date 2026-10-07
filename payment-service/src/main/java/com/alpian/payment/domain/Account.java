package com.alpian.payment.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * An account holding funds in a single currency.
 *
 * <p>One account belongs to exactly one user; a user may hold several. The balance carries its own
 * currency rather than being a bare amount alongside a separate currency field, which makes the two
 * impossible to disagree.
 */
public record Account(
    AccountId id, UserId userId, Money balance, Instant createdAt, Instant lastUpdatedAt) {

  public Account {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(userId, "userId");
    Objects.requireNonNull(balance, "balance");
    Objects.requireNonNull(createdAt, "createdAt");
    Objects.requireNonNull(lastUpdatedAt, "lastUpdatedAt");
  }

  /** Whether this account belongs to the given user. */
  public boolean isOwnedBy(UserId candidate) {
    return userId.equals(candidate);
  }

  /** Whether the balance covers {@code amount} in full. Assumes a matching currency. */
  public boolean canCover(Money amount) {
    return balance.isAtLeast(amount);
  }
}
