package com.alpian.payment.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.alpian.payment.domain.Money;
import com.alpian.payment.support.ApplicationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * The HTTP API end to end: real controllers, real service, real PostgreSQL. Where {@link
 * PaymentControllerTest} pins the contract with the service mocked, this checks that the pieces
 * agree with each other once assembled.
 */
@AutoConfigureMockMvc
class PaymentApiIntegrationTest extends ApplicationTestBase {

  @Autowired MockMvc mvc;

  private static String body(String amount, String currency) {
    return """
        {
          "amount": { "value": "%s", "currency": "%s" },
          "beneficiary": { "name": "Acme GmbH", "iban": "CH9300762011623852957" },
          "reference": "Invoice 42"
        }
        """
        .formatted(amount, currency);
  }

  private MockHttpServletRequestBuilder submit(Fixture f, String key, String body) {
    return post("/api/v1/users/{u}/accounts/{a}/payments", f.user().value(), f.account().value())
        .header("Idempotency-Key", key)
        .contentType(MediaType.APPLICATION_JSON)
        .content(body);
  }

  @Test
  @DisplayName("submit, retrieve, and see the balance reflect it")
  void fullPaymentLifecycle() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    String location =
        mvc.perform(submit(f, "lifecycle", body("250.50", "CHF")))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.status").value("COMPLETED"))
            .andExpect(jsonPath("$.amount.value").value("250.50"))
            .andReturn()
            .getResponse()
            .getHeader("Location");

    assertThat(location).isNotNull();
    mvc.perform(get(location))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("COMPLETED"));

    mvc.perform(
            get("/api/v1/users/{u}/accounts/{a}/balance", f.user().value(), f.account().value()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.balance.value").value("749.50"));
  }

  @Test
  @DisplayName("a retried request is replayed with the original response and debits once")
  void retryIsReplayedNotRepeated() throws Exception {
    Fixture f = givenAccount("1000.00", "CHF");

    String first =
        mvc.perform(submit(f, "retry", body("100.00", "CHF")))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getContentAsString();

    String second =
        mvc.perform(submit(f, "retry", body("100.00", "CHF")))
            .andExpect(status().isCreated())
            .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"))
            .andReturn()
            .getResponse()
            .getContentAsString();

    // The client that missed the first response gets that exact response.
    assertThat(second).isEqualTo(first);
    assertThat(accounts.findById(f.account()).orElseThrow().balance())
        .isEqualTo(Money.of("900.00", "CHF"));
  }

  @Test
  @DisplayName("a declined payment is 409 and remains retrievable as a FAILED attempt")
  void declinedPaymentIsRecordedAndRetrievable() throws Exception {
    Fixture f = givenAccount("10.00", "CHF");

    String location =
        mvc.perform(submit(f, "declined", body("50.00", "CHF")))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.code").value("insufficient_funds"))
            .andReturn()
            .getResponse()
            .getHeader("Location");

    mvc.perform(get(location))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("FAILED"))
        .andExpect(jsonPath("$.failureReason").value("Insufficient funds"));
  }

  @Test
  @DisplayName("another user can read neither the balance nor a payment of an account")
  void othersCannotReadAnAccount() throws Exception {
    Fixture owner = givenAccount("1000.00", "CHF");
    Fixture intruder = givenAccount("1000.00", "CHF");

    String location =
        mvc.perform(submit(owner, "private", body("10.00", "CHF")))
            .andExpect(status().isCreated())
            .andReturn()
            .getResponse()
            .getHeader("Location");
    String paymentId = location.substring(location.lastIndexOf('/') + 1);

    mvc.perform(
            get(
                "/api/v1/users/{u}/accounts/{a}/balance",
                intruder.user().value(),
                owner.account().value()))
        .andExpect(status().isNotFound());

    mvc.perform(
            get(
                "/api/v1/users/{u}/accounts/{a}/payments/{p}",
                intruder.user().value(),
                owner.account().value(),
                paymentId))
        .andExpect(status().isNotFound());

    // Pairing the payment id with the intruder's OWN account must not work either.
    mvc.perform(
            get(
                "/api/v1/users/{u}/accounts/{a}/payments/{p}",
                intruder.user().value(),
                intruder.account().value(),
                paymentId))
        .andExpect(status().isNotFound());
  }

  @Test
  @DisplayName("replaying another user's idempotency key reveals nothing")
  void replayCannotBeUsedToReadAnotherUsersPayment() throws Exception {
    Fixture owner = givenAccount("1000.00", "CHF");
    Fixture intruder = givenAccount("1000.00", "CHF");
    mvc.perform(submit(owner, "owners-key", body("10.00", "CHF"))).andExpect(status().isCreated());

    mvc.perform(
            post(
                    "/api/v1/users/{u}/accounts/{a}/payments",
                    intruder.user().value(),
                    owner.account().value())
                .header("Idempotency-Key", "owners-key")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body("10.00", "CHF")))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("account_not_found"))
        .andExpect(jsonPath("$.paymentId").doesNotExist());
  }

  @Test
  @DisplayName("the OpenAPI document is served and describes the payment endpoint")
  void servesTheOpenApiDocument() throws Exception {
    mvc.perform(get("/v3/api-docs"))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .string(containsString("/api/v1/users/{userId}/accounts/{accountId}/payments")))
        .andExpect(content().string(containsString("Idempotency-Key")))
        .andExpect(content().string(containsString("Authentication is out of scope")));
  }

  @Test
  void servesSwaggerUi() throws Exception {
    mvc.perform(get("/swagger-ui/index.html")).andExpect(status().isOk());
  }
}
