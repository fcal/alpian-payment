package com.alpian.payment.domain;

import java.util.Objects;
import java.util.UUID;

/**
 * Identifies a user.
 *
 * <p>Wrapped rather than passed as a bare {@link UUID} so that transposing two identifiers of
 * different kinds is a compile error. Several methods here take both a user and an account id,
 * where that mistake would otherwise be silent and the resulting bug subtle.
 */
public record UserId(UUID value) {
  public UserId {
    Objects.requireNonNull(value, "value");
  }

  public static UserId of(String value) {
    return new UserId(UUID.fromString(value));
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
