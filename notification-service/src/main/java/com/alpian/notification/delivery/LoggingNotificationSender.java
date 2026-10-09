package com.alpian.notification.delivery;

import com.alpian.payment.events.v1.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stands in for a real notification provider by logging the message.
 *
 * <p>This is where an email, SMS or push provider would be called, with the user's contact details
 * looked up from {@code user_id}, and {@code payment_id} passed as the provider's idempotency key.
 */
@Component
class LoggingNotificationSender implements NotificationSender {

  private static final Logger log = LoggerFactory.getLogger(LoggingNotificationSender.class);

  @Override
  public void send(NotificationEvent notification) {
    log.info(
        "Notifying user {} of payment {}: {}",
        notification.getUserId(),
        notification.getPaymentId(),
        notification.getMessage());
  }
}
