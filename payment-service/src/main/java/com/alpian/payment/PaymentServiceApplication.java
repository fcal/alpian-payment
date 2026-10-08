package com.alpian.payment;

import com.alpian.payment.config.PaymentProperties;
import com.alpian.payment.outbox.OutboxProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Entry point for the payment service: REST API, balance debit, and the outbox relay. */
@SpringBootApplication
@EnableConfigurationProperties({PaymentProperties.class, OutboxProperties.class})
@EnableScheduling // drives the outbox relay poller
public class PaymentServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}
