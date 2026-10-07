package com.alpian.payment.domain;

import java.util.Objects;

/**
 * The external party receiving an outbound payment.
 *
 * <p>The counterparty is outside this system, so it is recorded as descriptive detail on the
 * journal row rather than referenced as an account. The IBAN is checked only for plausible shape:
 * genuine validation means the mod-97 checksum and per-country length rules, which is out of scope
 * here and would in practice be delegated to a payment rail that rejects the payment downstream.
 */
public record Beneficiary(String name, String iban) {

  public static final int MAX_NAME_LENGTH = 140;

  public Beneficiary {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(iban, "iban");

    if (name.isBlank()) {
      throw new IllegalArgumentException("Beneficiary name cannot be blank");
    }
    if (name.length() > MAX_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Beneficiary name exceeds %d characters".formatted(MAX_NAME_LENGTH));
    }
    if (iban.isBlank()) {
      throw new IllegalArgumentException("Beneficiary IBAN cannot be blank");
    }

    name = name.strip();
    iban = iban.replace(" ", "").toUpperCase(java.util.Locale.ROOT);
  }
}
