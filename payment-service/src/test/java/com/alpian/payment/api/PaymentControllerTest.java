package com.alpian.payment.api;

import static org.assertj.core.api.Assertions.assertThat;
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

import com.alpian.payment.domain.Account;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentRequest;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.PaymentStatus;
import com.alpian.payment.service.PaymentService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** The HTTP contract, with the service mocked. */
@WebMvcTest(PaymentController.class)
@Import(SimpleMeterRegistry.class)
class PaymentControllerTest {

  private static final UUID USER = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID ACCOUNT = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");
  private static final String BASE = "/api/v1/users/" + USER + "/accounts/" + ACCOUNT;
  private static final String BODY =
      """
      {
        "amount": { "value": "250.50", "currency": "CHF" },
        "beneficiary": { "name": "Acme GmbH", "iban": "CH93 0076 2011 6238 5295 7" },
        "reference": "Invoice 42"
      }
      """;

  @Autowired MockMvc mvc;
  @MockBean PaymentService service;

  private static Payment payment(PaymentStatus status) {
    return new Payment(
        UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        ACCOUNT,
        "key-1",
        new BigDecimal("250.5000"),
        "CHF",
        "Acme GmbH",
        "CH9300762011623852957",
        "Invoice 42",
        status,
        status == PaymentStatus.FAILED ? "Insufficient funds" : null,
        Instant.parse("2026-10-07T10:15:30Z"));
  }

  private void serviceReturns(PaymentOutcome outcome, PaymentStatus status) {
    when(service.submit(any()))
        .thenReturn(new PaymentResult(outcome, status == null ? null : payment(status)));
  }

  private static MockHttpServletRequestBuilder submit(String body) {
    return post(BASE + "/payments")
        .header("Idempotency-Key", "key-1")
        .contentType(MediaType.APPLICATION_JSON)
        .content(body);
  }

  @Test
  @DisplayName("a completed payment is 201 with a Location, and money is a decimal string")
  void completed() throws Exception {
    serviceReturns(PaymentOutcome.COMPLETED, PaymentStatus.COMPLETED);

    String json =
        mvc.perform(submit(BODY))
            .andExpect(status().isCreated())
            .andExpect(
                header().string("Location", endsWith("/cccccccc-cccc-cccc-cccc-cccccccccccc")))
            .andExpect(header().doesNotExist(PaymentController.REPLAYED_HEADER))
            .andExpect(jsonPath("$.status").value("COMPLETED"))
            .andExpect(jsonPath("$.failureReason").doesNotExist())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(json).contains("\"value\":\"250.50\"");
  }

  @Test
  void mapsTheRequestBody() throws Exception {
    serviceReturns(PaymentOutcome.COMPLETED, PaymentStatus.COMPLETED);
    ArgumentCaptor<PaymentRequest> captor = ArgumentCaptor.forClass(PaymentRequest.class);

    mvc.perform(submit(BODY.replace("\"250.50\"", "250.50"))).andExpect(status().isCreated());

    verify(service).submit(captor.capture());
    PaymentRequest request = captor.getValue();
    assertThat(request.userId()).isEqualTo(USER);
    assertThat(request.idempotencyKey()).isEqualTo("key-1");
    assertThat(request.amount()).isEqualByComparingTo("250.50");
    assertThat(request.beneficiaryIban()).isEqualTo("CH9300762011623852957");
  }

  @Test
  @DisplayName("a decline is 409 identifying the recorded attempt")
  void declined() throws Exception {
    serviceReturns(PaymentOutcome.INSUFFICIENT_FUNDS, PaymentStatus.FAILED);

    mvc.perform(submit(BODY))
        .andExpect(status().isConflict())
        .andExpect(header().string("Content-Type", "application/problem+json"))
        .andExpect(header().string("Location", endsWith("/cccccccc-cccc-cccc-cccc-cccccccccccc")))
        .andExpect(jsonPath("$.code").value("insufficient_funds"))
        .andExpect(jsonPath("$.paymentId").value("cccccccc-cccc-cccc-cccc-cccccccccccc"));
  }

  @Test
  @DisplayName("a replay returns the original status, flagged as a replay")
  void replays() throws Exception {
    serviceReturns(PaymentOutcome.REPLAYED, PaymentStatus.COMPLETED);
    mvc.perform(submit(BODY))
        .andExpect(status().isCreated())
        .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"));

    serviceReturns(PaymentOutcome.REPLAYED, PaymentStatus.FAILED);
    mvc.perform(submit(BODY))
        .andExpect(status().isConflict())
        .andExpect(header().string(PaymentController.REPLAYED_HEADER, "true"));
  }

  @Test
  void rejections() throws Exception {
    serviceReturns(PaymentOutcome.ACCOUNT_NOT_FOUND, null);
    mvc.perform(submit(BODY))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("account_not_found"));

    serviceReturns(PaymentOutcome.CURRENCY_MISMATCH, null);
    mvc.perform(submit(BODY))
        .andExpect(status().isUnprocessableEntity())
        .andExpect(jsonPath("$.code").value("currency_mismatch"));

    serviceReturns(PaymentOutcome.IDEMPOTENCY_KEY_REUSED, null);
    mvc.perform(submit(BODY))
        .andExpect(status().isUnprocessableEntity())
        .andExpect(jsonPath("$.code").value("idempotency_key_reused"));
  }

  @Test
  @DisplayName("lock contention is a retryable 503 with Retry-After")
  void lockTimeout() throws Exception {
    when(service.submit(any())).thenThrow(new CannotAcquireLockException("lock_timeout"));

    mvc.perform(submit(BODY))
        .andExpect(status().isServiceUnavailable())
        .andExpect(header().string("Retry-After", "1"))
        .andExpect(jsonPath("$.code").value("lock_timeout"));
  }

  @Test
  void rejectsInvalidRequestsWithoutCallingTheService() throws Exception {
    // Missing Idempotency-Key.
    mvc.perform(post(BASE + "/payments").contentType(MediaType.APPLICATION_JSON).content(BODY))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("validation_failed"));
    // Over-long key.
    mvc.perform(submit(BODY).header("Idempotency-Key", "k".repeat(256)))
        .andExpect(status().isBadRequest());
    // Field errors are listed.
    mvc.perform(
            submit(
                """
                { "amount": { "value": "-5", "currency": "chf" },
                  "beneficiary": { "name": "", "iban": "CH93" } }
                """))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errors", hasSize(3)));
    // More than four decimals, an unknown currency, an unknown field, malformed JSON.
    mvc.perform(submit(BODY.replace("250.50", "250.12345"))).andExpect(status().isBadRequest());
    mvc.perform(submit(BODY.replace("CHF", "XYZ")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("validation_failed"));
    mvc.perform(submit(BODY.replace("reference", "refrence"))).andExpect(status().isBadRequest());
    mvc.perform(submit("{ not json")).andExpect(status().isBadRequest());

    verify(service, never()).submit(any());
  }

  @Test
  void getsAPayment() throws Exception {
    when(service.payment(any(), any(), any()))
        .thenReturn(Optional.of(payment(PaymentStatus.COMPLETED)));
    mvc.perform(get(BASE + "/payments/" + UUID.randomUUID()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.paymentId").value("cccccccc-cccc-cccc-cccc-cccccccccccc"));

    when(service.payment(any(), any(), any())).thenReturn(Optional.empty());
    mvc.perform(get(BASE + "/payments/" + UUID.randomUUID()))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("payment_not_found"));
  }

  @Test
  void getsTheBalance() throws Exception {
    when(service.account(any(), any()))
        .thenReturn(
            Optional.of(
                new Account(ACCOUNT, USER, new BigDecimal("1000.0000"), "CHF", Instant.now())));
    mvc.perform(get(BASE + "/balance"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.balance.value").value("1000.00"))
        .andExpect(jsonPath("$.balance.currency").value("CHF"));

    when(service.account(any(), any())).thenReturn(Optional.empty());
    mvc.perform(get(BASE + "/balance"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("account_not_found"));
  }
}
