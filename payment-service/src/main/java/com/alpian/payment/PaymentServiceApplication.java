package com.alpian.payment;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@OpenAPIDefinition(
    info =
        @Info(
            title = "Payment Service API",
            version = "v1",
            description =
                """
                Balance queries and outbound payments.

                Authentication is out of scope: the `userId` path segment stands in for the \
                authenticated user. Errors are RFC 9457 problem details with a stable `code` \
                property. Amounts are decimal strings."""))
public class PaymentServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}
