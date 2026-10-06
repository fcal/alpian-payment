package com.alpian.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Entry point for the payment service: REST API, balance debit, and the outbox relay. */
@SpringBootApplication
@EnableScheduling // drives the outbox relay poller
public class PaymentServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}
