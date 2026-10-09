package com.alpian.notification.streams;

import java.util.Locale;

/** Why a payment event was sent to the dead-letter topic instead of being notified. */
public enum DeadLetterReason {
  /** The value is not a decodable {@code PaymentEvent}, or is absent. */
  UNDECODABLE,
  /** The event decodes but lacks a field a notification needs, or has an unknown status. */
  INVALID_EVENT,
  /**
   * The record key is not the event's account id. Deduplication relies on every copy of a payment's
   * event landing on the same partition, which only the account-id key guarantees; a record keyed
   * otherwise could be checked against the wrong partition's store and notified twice.
   */
  KEY_MISMATCH;

  /** Value of the {@link DeadLetters#REASON} header. */
  public String code() {
    return name().toLowerCase(Locale.ROOT);
  }
}
