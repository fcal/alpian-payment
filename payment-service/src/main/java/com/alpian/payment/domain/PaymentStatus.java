package com.alpian.payment.domain;

/**
 * The persisted state of a payment in the journal.
 *
 * <p>Terminal states only. A payment is applied in a single short database transaction, so it is
 * never observably in flight and a {@code PENDING} state would be unreachable — modelling one would
 * imply a lifecycle the system does not have. A reservation-based flow against an external payment
 * rail would introduce it.
 *
 * <p>Distinct from {@link PaymentOutcome}, which describes what happened to a <em>request</em> and
 * includes cases that never produce a journal row at all.
 */
public enum PaymentStatus {
  /** Funds left the account. */
  COMPLETED,

  /** The attempt was recorded and declined; no funds moved. */
  FAILED
}
