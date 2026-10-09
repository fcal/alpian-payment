package com.alpian.notification.delivery;

import com.alpian.payment.events.v1.NotificationEvent;

/** Delivers a notification to the payer. */
public interface NotificationSender {

  /**
   * Delivers {@code notification}, or throws if it could not be delivered.
   *
   * <p>May be called more than once for the same notification: delivery is at-least-once (see
   * {@link NotificationListener}). An implementation calling a real provider should pass the
   * payment id as the provider's idempotency key, so a repeated call is not a repeated message.
   */
  void send(NotificationEvent notification);
}
