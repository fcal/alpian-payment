package com.alpian.payment.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.alpian.payment.support.ApplicationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** The HTTP API with the real service and database. */
@AutoConfigureMockMvc
class PaymentApiIntegrationTest extends ApplicationTestBase {

  @Autowired MockMvc mvc;

  private static MockHttpServletRequestBuilder pay(Fixture f, String key, String amount) {
    return post("/api/v1/users/{u}/accounts/{a}/payments", f.user(), f.account())
        .header("Idempotency-Key", key)
        .contentType(MediaType.APPLICATION_JSON)
        .content(
            """
            {"amount": {"value": "%s", "currency": "CHF"},
             "beneficiary": {"name": "Acme GmbH", "iban": "CH9300762011623852957"}}
            """
                .formatted(amount));
  }

  @Test
  @DisplayName("submit, retrieve, and see the balance reflect it")
  void lifecycle() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    String location =
        mvc.perform(pay(f, "lifecycle", "250.50"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.amount.value").value("250.50"))
            .andReturn()
            .getResponse()
            .getHeader("Location");

    mvc.perform(get(location))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("COMPLETED"));
    mvc.perform(get("/api/v1/users/{u}/accounts/{a}/balance", f.user(), f.account()))
        .andExpect(jsonPath("$.balance.value").value("749.50"));
  }

  @Test
  @DisplayName("a retry gets the identical response and debits once")
  void retryIsReplayed() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    String first =
        mvc.perform(pay(f, "retry", "100.00")).andReturn().getResponse().getContentAsString();
    String second =
        mvc.perform(pay(f, "retry", "100.00"))
            .andExpect(status().isCreated())
            .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"))
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertThat(second).isEqualTo(first);
    assertThat(balance(f.account())).isEqualByComparingTo("900.00");
  }

  @Test
  @DisplayName("a decline is 409 and stays retrievable as FAILED")
  void declineIsRetrievable() throws Exception {
    Fixture f = givenAccount("10.00", "CHF");

    String location =
        mvc.perform(pay(f, "declined", "50.00"))
            .andExpect(status().isConflict())
            .andReturn()
            .getResponse()
            .getHeader("Location");

    mvc.perform(get(location))
        .andExpect(jsonPath("$.status").value("FAILED"))
        .andExpect(jsonPath("$.failureReason").value("Insufficient funds"));
  }

  @Test
  @DisplayName("another user can read neither the balance nor the payments of an account")
  void othersCannotRead() throws Exception {
    Fixture owner = givenAccount("1000.00", "CHF");
    Fixture intruder = givenAccount("1000.00", "CHF");
    String location =
        mvc.perform(pay(owner, "private", "10.00")).andReturn().getResponse().getHeader("Location");
    String paymentId = location.substring(location.lastIndexOf('/') + 1);

    mvc.perform(get("/api/v1/users/{u}/accounts/{a}/balance", intruder.user(), owner.account()))
        .andExpect(status().isNotFound());
    mvc.perform(
            get(
                "/api/v1/users/{u}/accounts/{a}/payments/{p}",
                intruder.user(),
                owner.account(),
                paymentId))
        .andExpect(status().isNotFound());
    // Pairing the payment with the intruder's own account does not work either.
    mvc.perform(
            get(
                "/api/v1/users/{u}/accounts/{a}/payments/{p}",
                intruder.user(),
                intruder.account(),
                paymentId))
        .andExpect(status().isNotFound());
  }

  @Test
  @DisplayName("the OpenAPI document and Swagger UI are served")
  void servesApiDocumentation() throws Exception {
    mvc.perform(get("/v3/api-docs"))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .string(containsString("/api/v1/users/{userId}/accounts/{accountId}/payments")))
        .andExpect(content().string(containsString("Idempotency-Key")));
    mvc.perform(get("/swagger-ui/index.html")).andExpect(status().isOk());
  }
}
