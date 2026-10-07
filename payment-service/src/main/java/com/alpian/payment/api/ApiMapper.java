package com.alpian.payment.api;

import com.alpian.payment.api.dto.BalanceResponse;
import com.alpian.payment.api.dto.BeneficiaryDto;
import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.api.dto.MoneyDto;
import com.alpian.payment.api.dto.PaymentResponse;
import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.service.PaymentRequest;
import java.math.BigDecimal;
import java.util.Currency;

/**
 * Converts between the wire contract and the domain.
 *
 * <p>Hand-written rather than generated with MapStruct: the conversions are few and each carries a
 * decision — how a money value is rendered, which domain validation failures are the client's fault
 * — that reads more clearly as code than as mapping annotations.
 */
public final class ApiMapper {

  private ApiMapper() {}

  /**
   * @throws InvalidRequestException if a field is well formed but not a valid domain value, such as
   *     an unrecognised currency code
   */
  public static PaymentRequest toPaymentRequest(
      UserId userId, AccountId accountId, String idempotencyKey, CreatePaymentRequest body) {
    try {
      return new PaymentRequest(
          userId,
          accountId,
          new IdempotencyKey(idempotencyKey),
          new Money(body.amount().value(), Currency.getInstance(body.amount().currency())),
          new Beneficiary(body.beneficiary().name(), body.beneficiary().iban()),
          body.reference());
    } catch (IllegalArgumentException e) {
      // Only domain construction is inside this block, so any IllegalArgumentException here is
      // about the client's input -- unlike a global handler, which would also capture bugs.
      throw new InvalidRequestException(e.getMessage(), e);
    }
  }

  public static PaymentResponse toResponse(Payment payment) {
    return new PaymentResponse(
        payment.id().value(),
        payment.accountId().value(),
        payment.status().name(),
        toDto(payment.amount()),
        new BeneficiaryDto(payment.beneficiary().name(), payment.beneficiary().iban()),
        payment.reference(),
        payment.failureReason(),
        payment.createdAt());
  }

  public static BalanceResponse toBalanceResponse(Account account) {
    return new BalanceResponse(
        account.id().value(), toDto(account.balance()), account.lastUpdatedAt());
  }

  /**
   * Renders an amount at the currency's conventional precision, widening only when needed.
   *
   * <p>Stored values carry four decimal places, so {@code 250.50 CHF} is held as {@code 250.5000}.
   * Echoing that back is accurate but unhelpful, and trimming every trailing zero is worse: {@code
   * 250.5} reads as a typo for an amount in francs. So the value is shown at the currency's
   * minor-unit precision — two places for CHF, none for JPY — unless doing so would hide a non-zero
   * digit, in which case all significant places are kept. Information is never lost, only padding
   * removed.
   */
  static MoneyDto toDto(Money money) {
    BigDecimal stripped = money.amount().stripTrailingZeros();
    int scale = Math.max(money.currency().getDefaultFractionDigits(), stripped.scale());
    return new MoneyDto(money.amount().setScale(Math.max(scale, 0)), money.currencyCode());
  }
}
