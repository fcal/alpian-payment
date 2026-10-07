package com.alpian.payment.domain;

import java.util.Optional;

/**
 * The terminal outcome of a payment attempt, as reported to metrics.
 *
 * <p>This is the single vocabulary for "what happened to a payment": the metric tag values, the
 * {@code failure_reason} persisted on the journal row, and the error responses rendered by the API
 * layer all derive from it. Keeping one enum avoids the usual drift where a dashboard filters on an
 * outcome the API stopped producing two releases ago.
 *
 * <p>Note that an outcome is not the same as an HTTP status: {@link #IDEMPOTENT_REPLAY} returns the
 * original response, which may itself have been a success or a failure.
 */
public enum PaymentOutcome {
  /** Funds were debited and the payment recorded. */
  COMPLETED("completed"),

  /** The account existed and was valid, but held less than the requested amount. */
  INSUFFICIENT_FUNDS("insufficient_funds"),

  /** The payment currency differed from the account currency. No conversion is performed. */
  CURRENCY_MISMATCH("currency_mismatch"),

  /** No account with the requested identifier exists. */
  ACCOUNT_NOT_FOUND("account_not_found"),

  /**
   * The account exists but does not belong to the user in the request path. Tracked separately from
   * {@link #ACCOUNT_NOT_FOUND} because a non-zero rate here is a signal worth alerting on:
   * legitimate clients do not address other users' accounts.
   */
  ACCOUNT_NOT_OWNED("account_not_owned"),

  /** The request repeated an idempotency key, so the stored outcome was returned unchanged. */
  IDEMPOTENT_REPLAY("idempotent_replay"),

  /**
   * The idempotency key had already been used for a <em>different</em> payment. Replaying the
   * stored outcome would tell the client a payment it never made had succeeded, so the request is
   * refused instead.
   */
  IDEMPOTENCY_KEY_REUSED("idempotency_key_reused"),

  /**
   * The per-account row lock was not acquired within the configured timeout, so the request was
   * rejected rather than left queueing and holding a connection. A rising rate indicates contention
   * on a hot account.
   */
  LOCK_TIMEOUT("lock_timeout"),

  /** The request was malformed or violated a field constraint. */
  VALIDATION_FAILED("validation_failed");

  private final String tagValue;

  PaymentOutcome(String tagValue) {
    this.tagValue = tagValue;
  }

  /** Lower snake case, matching Prometheus label-value convention. */
  public String tagValue() {
    return tagValue;
  }

  /** Whether the payment resulted in funds leaving the account. */
  public boolean isSuccess() {
    return this == COMPLETED;
  }

  /**
   * The journal status this outcome is recorded under, if it produces a journal row at all.
   *
   * <p>Most outcomes are rejections that never became a payment attempt — an unknown account or a
   * malformed request leaves nothing to record. {@link #IDEMPOTENT_REPLAY} is also absent, since a
   * replay returns an existing row rather than writing one.
   */
  public Optional<PaymentStatus> journalStatus() {
    return switch (this) {
      case COMPLETED -> Optional.of(PaymentStatus.COMPLETED);
      case INSUFFICIENT_FUNDS -> Optional.of(PaymentStatus.FAILED);
      case CURRENCY_MISMATCH,
              ACCOUNT_NOT_FOUND,
              ACCOUNT_NOT_OWNED,
              IDEMPOTENT_REPLAY,
              IDEMPOTENCY_KEY_REUSED,
              LOCK_TIMEOUT,
              VALIDATION_FAILED ->
          Optional.empty();
    };
  }
}
