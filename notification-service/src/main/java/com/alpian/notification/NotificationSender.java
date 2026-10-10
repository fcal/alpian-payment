package com.alpian.notification;

import com.alpian.payment.events.v1.NotificationEvent;

/**
 * Delivers a notification, or throws. Delivery is at least once, so a real provider should get the
 * payment id as its idempotency key.
 */
public interface NotificationSender {

  void send(NotificationEvent notification);
}
