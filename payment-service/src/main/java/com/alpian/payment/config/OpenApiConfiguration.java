package com.alpian.payment.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * API metadata for the generated OpenAPI document, served at {@code /v3/api-docs} with Swagger UI
 * at {@code /swagger-ui.html}.
 *
 * <p>Generated from the code rather than written first and generated into it. Spec-first is the
 * stronger discipline when several teams consume an API, since the contract is reviewed before any
 * implementation exists; for a single service at this size, deriving it from annotated controllers
 * keeps the two from drifting with no extra tooling.
 */
@Configuration
public class OpenApiConfiguration {

  @Bean
  public OpenAPI paymentServiceApi() {
    return new OpenAPI()
        .info(
            new Info()
                .title("Payment Service API")
                .version("v1")
                .description(
                    """
                    Balance queries and outbound payments.

                    **Authentication is out of scope for this exercise and not implemented.** \
                    The `userId` path segment stands in for the subject of a validated JWT; in \
                    production it must come from the token, never from the URL.

                    Errors use RFC 9457 problem details. Branch on the `code` property, which is \
                    stable; `detail` is prose and may change.

                    Monetary values are decimal strings in responses, to keep them out of binary \
                    floating point on the client."""));
  }
}
