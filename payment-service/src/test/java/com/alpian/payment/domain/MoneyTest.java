package com.alpian.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Currency;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MoneyTest {

  private static final Currency CHF = Currency.getInstance("CHF");
  private static final Currency EUR = Currency.getInstance("EUR");

  @Test
  @DisplayName("amounts equal in value are equal regardless of how they were written")
  void normalisesScaleSoEqualityBehavesAsExpected() {
    // The reason scale is normalised: BigDecimal compares scale as well as value, so without
    // this, two Money instances for the same amount would be unequal and every assertion and
    // map lookup involving money would be subtly unreliable.
    assertThat(Money.of("10", "CHF")).isEqualTo(Money.of("10.0000", "CHF"));
    assertThat(Money.of("10.5", "CHF")).isEqualTo(Money.of("10.50", "CHF"));
    assertThat(Money.of("10", "CHF")).hasSameHashCodeAs(Money.of("10.00", "CHF"));
  }

  @Test
  void normalisesToTheScaleOfTheDatabaseColumn() {
    assertThat(Money.of("10.5", "CHF").amount().scale()).isEqualTo(Money.SCALE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"-0.01", "-1", "-999999"})
  @DisplayName("negative amounts are rejected outright")
  void rejectsNegativeAmounts(String amount) {
    assertThatThrownBy(() -> Money.of(amount, "CHF"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  @DisplayName("more precision than the column can hold is rejected, not silently rounded")
  void rejectsExcessivePrecision() {
    // Rounding here would lose money quietly. Half a cent per payment is a real defect class.
    assertThatThrownBy(() -> Money.of("1.00005", "CHF"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("decimal places");
  }

  @Test
  void rejectsUnknownCurrencyCodes() {
    assertThatThrownBy(() -> Money.of("1.00", "XYZ")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void subtractsWithinTheSameCurrency() {
    assertThat(Money.of("100.00", "CHF").subtract(Money.of("30.50", "CHF")))
        .isEqualTo(Money.of("69.50", "CHF"));
  }

  @Test
  @DisplayName("subtracting below zero fails loudly rather than producing a negative balance")
  void refusesToSubtractBelowZero() {
    // Callers establish sufficiency first, so this is a missing check -- a bug, not a business
    // outcome. Returning a negative Money would let it propagate to the database.
    assertThatThrownBy(() -> Money.of("10.00", "CHF").subtract(Money.of("10.01", "CHF")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  @DisplayName("arithmetic and comparison across currencies is refused, never coerced")
  void refusesCrossCurrencyOperations() {
    Money chf = Money.of("10.00", "CHF");
    Money eur = Money.of("10.00", "EUR");

    assertThatThrownBy(() -> chf.subtract(eur)).hasMessageContaining("Currency mismatch");
    assertThatThrownBy(() -> chf.add(eur)).hasMessageContaining("Currency mismatch");
    // Comparing by numeric value alone would make 10 CHF "equal to" 10 EUR, which is wrong
    // rather than approximate.
    assertThatThrownBy(() -> chf.compareTo(eur)).hasMessageContaining("Currency mismatch");

    assertThat(chf).isNotEqualTo(eur);
    assertThat(chf.hasSameCurrencyAs(eur)).isFalse();
  }

  @Test
  void comparesAmountsWithinACurrency() {
    Money ten = Money.of("10.00", "CHF");

    assertThat(ten.isAtLeast(Money.of("10.00", "CHF"))).isTrue();
    assertThat(ten.isAtLeast(Money.of("9.99", "CHF"))).isTrue();
    assertThat(ten.isAtLeast(Money.of("10.01", "CHF"))).isFalse();
  }

  @Test
  void reportsZeroAndPositive() {
    assertThat(Money.zero(CHF).isZero()).isTrue();
    assertThat(Money.zero(CHF).isPositive()).isFalse();
    assertThat(Money.of("0.0001", "CHF").isPositive()).isTrue();
  }

  @Test
  void rendersAmountAndCurrency() {
    assertThat(Money.of("1234.5", "EUR")).hasToString("1234.5000 EUR");
    assertThat(Money.zero(EUR).currencyCode()).isEqualTo("EUR");
  }

  @Test
  @DisplayName("no plain-value construction bypasses validation")
  void validatesTheCanonicalConstructorToo() {
    assertThatThrownBy(() -> new Money(new BigDecimal("-1"), CHF))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
