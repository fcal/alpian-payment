package com.alpian.notification;

import com.alpian.payment.events.v1.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Stands in for an email, SMS or push provider. */
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
