package com.alpian.payment.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;

/**
 * A monetary amount in a single currency.
 *
 * <p>Backed by {@link BigDecimal}, never a floating point type: binary floats cannot represent
 * decimal fractions such as {@code 0.10} exactly and the error compounds across arithmetic.
 *
 * <p>The scale is normalised to {@link #SCALE} on construction, for two reasons. It matches the
 * {@code NUMERIC(19,4)} columns the value is read from and written to, so a round trip through the
 * database does not change the object. More subtly, it makes equality behave as a reader expects:
 * {@code BigDecimal} considers {@code 10.0} and {@code 10.00} unequal because it compares scale as
 * well as value, so without normalisation two {@code Money} instances for the same amount could
 * legitimately fail to be equal.
 *
 * <p>Negative amounts are rejected. No negative monetary quantity is meaningful here — a balance
 * cannot go below zero (the database enforces it) and a payment amount must be positive — so
 * rejecting them turns a whole class of sign-error bug into an immediate failure.
 */
public record Money(BigDecimal amount, Currency currency) implements Comparable<Money> {

  /** Matches the {@code NUMERIC(19,4)} columns in the schema. */
  public static final int SCALE = 4;

  public Money {
    Objects.requireNonNull(amount, "amount");
    Objects.requireNonNull(currency, "currency");

    if (amount.signum() < 0) {
      throw new IllegalArgumentException("Monetary amount cannot be negative: " + amount);
    }
    if (amount.scale() > SCALE) {
      throw new IllegalArgumentException(
          "Monetary amount has more than %d decimal places: %s".formatted(SCALE, amount));
    }
    amount = amount.setScale(SCALE);
  }

  /**
   * @param amount decimal string, e.g. {@code "1234.56"}
   * @param currencyCode ISO 4217 alphabetic code, e.g. {@code "CHF"}
   * @throws IllegalArgumentException if the currency code is not a known ISO 4217 code
   */
  public static Money of(String amount, String currencyCode) {
    return new Money(new BigDecimal(amount), Currency.getInstance(currencyCode));
  }

  public static Money zero(Currency currency) {
    return new Money(BigDecimal.ZERO, currency);
  }

  /** ISO 4217 alphabetic code, as stored in the database. */
  public String currencyCode() {
    return currency.getCurrencyCode();
  }

  public boolean isZero() {
    return amount.signum() == 0;
  }

  public boolean isPositive() {
    return amount.signum() > 0;
  }

  /** Whether this amount covers {@code other} in full. */
  public boolean isAtLeast(Money other) {
    return compareTo(other) >= 0;
  }

  /**
   * @throws IllegalArgumentException if {@code other} would take the result below zero. Callers
   *     establish sufficiency first, so this indicates a missing check rather than a business
   *     outcome, and failing loudly is preferable to producing a negative balance.
   */
  public Money subtract(Money other) {
    requireSameCurrency(other);
    return new Money(amount.subtract(other.amount), currency);
  }

  public Money add(Money other) {
    requireSameCurrency(other);
    return new Money(amount.add(other.amount), currency);
  }

  /**
   * Orders by amount.
   *
   * @throws IllegalArgumentException if the currencies differ — amounts in different currencies are
   *     not comparable without an exchange rate, and silently ordering them by numeric value would
   *     be wrong rather than merely imprecise.
   */
  @Override
  public int compareTo(Money other) {
    requireSameCurrency(other);
    return amount.compareTo(other.amount);
  }

  public boolean hasSameCurrencyAs(Money other) {
    return currency.equals(other.currency);
  }

  private void requireSameCurrency(Money other) {
    Objects.requireNonNull(other, "other");
    if (!hasSameCurrencyAs(other)) {
      throw new IllegalArgumentException(
          "Currency mismatch: %s and %s".formatted(currencyCode(), other.currencyCode()));
    }
  }

  @Override
  public String toString() {
    return amount.toPlainString() + " " + currencyCode();
  }
}
