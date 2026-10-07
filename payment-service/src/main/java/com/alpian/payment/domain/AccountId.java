package com.alpian.payment.domain;

import java.util.Objects;
import java.util.UUID;

/** Identifies an account. See {@link UserId} for why identifiers are wrapped. */
public record AccountId(UUID value) {
  public AccountId {
    Objects.requireNonNull(value, "value");
  }

  public static AccountId of(String value) {
    return new AccountId(UUID.fromString(value));
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
