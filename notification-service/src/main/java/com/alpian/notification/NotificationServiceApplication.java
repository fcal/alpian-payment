package com.alpian.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the notification service: consumes payment events, deduplicates them, and
 * notifies the sender.
 */
@SpringBootApplication
public class NotificationServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(NotificationServiceApplication.class, args);
  }
}
