package com.alpian.payment.api;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Beneficiary;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Money;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.observability.PaymentMetrics;
import com.alpian.payment.service.AccountService;
import com.alpian.payment.service.PaymentRequest;
import com.alpian.payment.service.PaymentService;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * Web-layer tests: the HTTP contract, with the service mocked.
 *
 * <p>The service is mocked so each {@link PaymentResult} variant can be produced on demand and its
 * mapping to status, headers and body asserted precisely. The service's own behaviour is covered
 * elsewhere; this class is about what a client actually receives.
 */
@WebMvcTest({PaymentController.class, AccountController.class})
class PaymentControllerTest {

  private static final UUID USER = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID ACCOUNT = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");
  private static final String PAYMENTS =
      "/api/v1/users/" + USER + "/accounts/" + ACCOUNT + "/payments";

  private static final String VALID_BODY =
      """
      {
        "amount": { "value": "250.50", "currency": "CHF" },
        "beneficiary": { "name": "Acme GmbH", "iban": "CH93 0076 2011 6238 5295 7" },
        "reference": "Invoice 42"
      }
      """;

  @Autowired MockMvc mvc;
  @MockBean PaymentService payments;
  @MockBean AccountService queries;
  @MockBean PaymentMetrics metrics;

  private static Payment completedPayment() {
    return Payment.completed(
        PaymentId.of("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        new AccountId(ACCOUNT),
        new IdempotencyKey("key-1"),
        Money.of("250.50", "CHF"),
        new Beneficiary("Acme GmbH", "CH9300762011623852957"),
        "Invoice 42",
        Instant.parse("2026-10-07T10:15:30Z"));
  }

  private static Payment failedPayment() {
    return Payment.failed(
        PaymentId.of("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        new AccountId(ACCOUNT),
        new IdempotencyKey("key-1"),
        Money.of("250.50", "CHF"),
        new Beneficiary("Acme GmbH", "CH9300762011623852957"),
        "Invoice 42",
        "Insufficient funds",
        Instant.parse("2026-10-07T10:15:30Z"));
  }

  private MockHttpServletRequestBuilder submit(String body) {
    return post(PAYMENTS)
        .header("Idempotency-Key", "key-1")
        .contentType(MediaType.APPLICATION_JSON)
        .content(body);
  }

  @Nested
  class Success {

    @Test
    @DisplayName("a completed payment is 201 with a Location for the new resource")
    void completedIs201WithLocation() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Completed(completedPayment()));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isCreated())
          .andExpect(
              header()
                  .string("Location", endsWith(PAYMENTS + "/cccccccc-cccc-cccc-cccc-cccccccccccc")))
          .andExpect(header().doesNotExist(PaymentController.REPLAYED_HEADER))
          .andExpect(jsonPath("$.paymentId").value("cccccccc-cccc-cccc-cccc-cccccccccccc"))
          .andExpect(jsonPath("$.status").value("COMPLETED"))
          .andExpect(jsonPath("$.beneficiary.iban").value("CH9300762011623852957"))
          .andExpect(jsonPath("$.failureReason").doesNotExist());
    }

    @Test
    @DisplayName("money is a decimal string at the currency's precision, never a JSON number")
    void rendersMoneyAsAString() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Completed(completedPayment()));

      MvcResult result = mvc.perform(submit(VALID_BODY)).andReturn();

      // Asserted on the raw text: jsonPath would happily coerce a number to the same value and
      // the test would pass while the contract was broken.
      String json = result.getResponse().getContentAsString();
      org.assertj.core.api.Assertions.assertThat(json)
          .contains("\"value\":\"250.50\"")
          .doesNotContain("\"value\":250");
    }

    @Test
    @DisplayName("the request is mapped into the domain faithfully, IBAN spacing normalised")
    void mapsTheRequestIntoTheDomain() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Completed(completedPayment()));
      var captor = org.mockito.ArgumentCaptor.forClass(PaymentRequest.class);

      mvc.perform(submit(VALID_BODY)).andExpect(status().isCreated());

      verify(payments).submit(captor.capture());
      PaymentRequest mapped = captor.getValue();
      org.assertj.core.api.Assertions.assertThat(mapped.userId().value()).isEqualTo(USER);
      org.assertj.core.api.Assertions.assertThat(mapped.accountId().value()).isEqualTo(ACCOUNT);
      org.assertj.core.api.Assertions.assertThat(mapped.idempotencyKey().value())
          .isEqualTo("key-1");
      org.assertj.core.api.Assertions.assertThat(mapped.amount())
          .isEqualTo(Money.of("250.50", "CHF"));
      org.assertj.core.api.Assertions.assertThat(mapped.beneficiary().iban())
          .isEqualTo("CH9300762011623852957");
    }

    @Test
    @DisplayName("an amount sent as a JSON number is accepted and read exactly")
    void acceptsANumericAmount() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Completed(completedPayment()));

      mvc.perform(submit(VALID_BODY.replace("\"250.50\"", "250.50")))
          .andExpect(status().isCreated());
    }
  }

  @Nested
  class Idempotency {

    @Test
    @DisplayName("replaying a completed payment returns the original 201, flagged as a replay")
    void replayOfCompletedIs201() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Replayed(completedPayment()));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isCreated())
          .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"))
          .andExpect(jsonPath("$.paymentId").value("cccccccc-cccc-cccc-cccc-cccccccccccc"));
    }

    @Test
    @DisplayName("replaying a declined payment returns the original 409, flagged as a replay")
    void replayOfDeclinedIs409() throws Exception {
      when(payments.submit(any())).thenReturn(new PaymentResult.Replayed(failedPayment()));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isConflict())
          .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"))
          .andExpect(jsonPath("$.code").value("insufficient_funds"));
    }

    @Test
    void keyReusedForADifferentPaymentIs422() throws Exception {
      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Rejected(
                  PaymentOutcome.IDEMPOTENCY_KEY_REUSED,
                  "Idempotency key has already been used for a different payment"));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isUnprocessableEntity())
          .andExpect(jsonPath("$.code").value("idempotency_key_reused"));
    }
  }

  @Nested
  class BusinessFailures {

    @Test
    @DisplayName("insufficient funds is 409, identifying the recorded attempt")
    void declinedIs409WithPaymentId() throws Exception {
      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Declined(failedPayment(), PaymentOutcome.INSUFFICIENT_FUNDS));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isConflict())
          .andExpect(header().string("Content-Type", "application/problem+json"))
          .andExpect(header().string("Location", endsWith("/dddddddd-dddd-dddd-dddd-dddddddddddd")))
          .andExpect(jsonPath("$.code").value("insufficient_funds"))
          .andExpect(jsonPath("$.paymentId").value("dddddddd-dddd-dddd-dddd-dddddddddddd"))
          .andExpect(jsonPath("$.type").value("/problems/insufficient-funds"));
    }

    @Test
    void currencyMismatchIs422() throws Exception {
      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Rejected(
                  PaymentOutcome.CURRENCY_MISMATCH,
                  "Payment currency EUR does not match account currency CHF"));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isUnprocessableEntity())
          .andExpect(jsonPath("$.code").value("currency_mismatch"));
    }

    @Test
    @DisplayName("lock contention is a retryable 503 with Retry-After")
    void lockTimeoutIs503WithRetryAfter() throws Exception {
      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Rejected(
                  PaymentOutcome.LOCK_TIMEOUT, "Another payment on this account is in progress"));

      mvc.perform(submit(VALID_BODY))
          .andExpect(status().isServiceUnavailable())
          .andExpect(header().string("Retry-After", "1"))
          .andExpect(jsonPath("$.code").value("lock_timeout"));
    }

    @Test
    @DisplayName("an account owned by someone else is indistinguishable from a missing one")
    void notOwnedAndNotFoundProduceIdenticalResponses() throws Exception {
      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_FOUND, "Account not found"));
      String notFound =
          mvc.perform(submit(VALID_BODY))
              .andExpect(status().isNotFound())
              .andReturn()
              .getResponse()
              .getContentAsString();

      when(payments.submit(any()))
          .thenReturn(
              new PaymentResult.Rejected(PaymentOutcome.ACCOUNT_NOT_OWNED, "Account not found"));
      String notOwned =
          mvc.perform(submit(VALID_BODY))
              .andExpect(status().isNotFound())
              .andReturn()
              .getResponse()
              .getContentAsString();

      // Byte-identical, `code` included. Mapping the outcome straight to `code` would have sent
      // "account_not_owned" to the client and confirmed the account exists.
      org.assertj.core.api.Assertions.assertThat(notOwned).isEqualTo(notFound);
      org.assertj.core.api.Assertions.assertThat(notOwned).doesNotContain("not_owned");
    }
  }

  @Nested
  class Validation {

    @Test
    @DisplayName("a missing Idempotency-Key is refused before the service is called")
    void requiresIdempotencyKey() throws Exception {
      mvc.perform(post(PAYMENTS).contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
          .andExpect(status().isBadRequest())
          .andExpect(jsonPath("$.code").value("validation_failed"));
      verify(payments, never()).submit(any());
      verify(metrics).recordAttempt(PaymentOutcome.VALIDATION_FAILED);
    }

    @Test
    void rejectsAnOverlongIdempotencyKey() throws Exception {
      mvc.perform(
              post(PAYMENTS)
                  .header("Idempotency-Key", "k".repeat(256))
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(VALID_BODY))
          .andExpect(status().isBadRequest())
          .andExpect(jsonPath("$.code").value("validation_failed"));
      verify(payments, never()).submit(any());
    }

    @Test
    @DisplayName("field errors are itemised so a client can show them against each field")
    void itemisesFieldErrors() throws Exception {
      String body =
          """
          { "amount": { "value": "-5", "currency": "chf" },
            "beneficiary": { "name": "", "iban": "CH93" } }
          """;

      mvc.perform(submit(body))
          .andExpect(status().isBadRequest())
          .andExpect(jsonPath("$.code").value("validation_failed"))
          .andExpect(jsonPath("$.errors", hasSize(3)));
      verify(payments, never()).submit(any());
    }

    @Test
    @DisplayName("more than four decimal places is refused rather than rounded")
    void rejectsExcessPrecision() throws Exception {
      mvc.perform(submit(VALID_BODY.replace("\"250.50\"", "\"250.12345\"")))
          .andExpect(status().isBadRequest());
      verify(payments, never()).submit(any());
    }

    @Test
    @DisplayName("a well-formed but unknown currency code is the client's error, not a 500")
    void rejectsUnknownCurrency() throws Exception {
      mvc.perform(submit(VALID_BODY.replace("\"CHF\"", "\"XYZ\"")))
          .andExpect(status().isBadRequest())
          .andExpect(jsonPath("$.code").value("validation_failed"));
    }

    @Test
    void rejectsMalformedJson() throws Exception {
      mvc.perform(submit("{ not json")).andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("an unknown field is refused, catching a misspelt field the client meant to send")
    void rejectsUnknownFields() throws Exception {
      mvc.perform(submit(VALID_BODY.replace("\"reference\"", "\"refrence\"")))
          .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsANonUuidPathSegment() throws Exception {
      mvc.perform(
              post("/api/v1/users/not-a-uuid/accounts/" + ACCOUNT + "/payments")
                  .header("Idempotency-Key", "key-1")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(VALID_BODY))
          .andExpect(status().isBadRequest());
    }
  }

  @Nested
  class Queries {

    @Test
    void returnsAPayment() throws Exception {
      Payment payment = completedPayment();
      when(queries.payment(any(), any(), any())).thenReturn(Optional.of(payment));

      mvc.perform(get(PAYMENTS + "/" + payment.id()))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.paymentId").value(payment.id().toString()));
    }

    @Test
    void missingPaymentIs404() throws Exception {
      when(queries.payment(any(), any(), any())).thenReturn(Optional.empty());

      mvc.perform(get(PAYMENTS + "/" + UUID.randomUUID()))
          .andExpect(status().isNotFound())
          .andExpect(jsonPath("$.code").value("payment_not_found"));
    }

    @Test
    void returnsTheBalance() throws Exception {
      when(queries.ownedAccount(any(), any()))
          .thenReturn(
              Optional.of(
                  new com.alpian.payment.domain.Account(
                      new AccountId(ACCOUNT),
                      new com.alpian.payment.domain.UserId(USER),
                      Money.of("1000", "CHF"),
                      Instant.parse("2026-10-01T00:00:00Z"),
                      Instant.parse("2026-10-07T10:15:30Z"))));

      mvc.perform(get("/api/v1/users/" + USER + "/accounts/" + ACCOUNT + "/balance"))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.balance.value").value("1000.00"))
          .andExpect(jsonPath("$.balance.currency").value("CHF"));
    }

    @Test
    void unknownAccountBalanceIs404() throws Exception {
      when(queries.ownedAccount(any(), any())).thenReturn(Optional.empty());

      mvc.perform(get("/api/v1/users/" + USER + "/accounts/" + ACCOUNT + "/balance"))
          .andExpect(status().isNotFound())
          .andExpect(jsonPath("$.code").value("account_not_found"));
    }
  }
}
