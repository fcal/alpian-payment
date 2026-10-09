package com.alpian.payment.api;

import static com.alpian.payment.api.ApiExceptionHandler.problem;

import com.alpian.payment.api.dto.BalanceResponse;
import com.alpian.payment.api.dto.BeneficiaryDto;
import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.api.dto.MoneyDto;
import com.alpian.payment.api.dto.PaymentResponse;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentRequest;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.service.PaymentService;
import io.micrometer.core.instrument.MeterRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Currency;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Payments and balances. The {@code userId} path segment stands in for the authenticated user,
 * which would come from a validated token in production.
 */
@RestController
@RequestMapping("/api/v1/users/{userId}/accounts/{accountId}")
public class PaymentController {

  static final String REPLAYED_HEADER = "Idempotent-Replayed";

  private final PaymentService service;
  private final MeterRegistry metrics;

  public PaymentController(PaymentService service, MeterRegistry metrics) {
    this.service = service;
    this.metrics = metrics;
  }

  @PostMapping("/payments")
  @Operation(
      summary = "Submit an outbound payment",
      description =
          """
          Debits the account and records the payment, or declines it. Idempotent: resending \
          the same `Idempotency-Key` returns the original response with \
          `Idempotent-Replayed: true` and never debits twice.""")
  @ApiResponse(
      responseCode = "201",
      description = "Completed",
      headers = @Header(name = REPLAYED_HEADER, description = "true on a replay"),
      content = @Content(schema = @Schema(implementation = PaymentResponse.class)))
  @ApiResponse(responseCode = "400", description = "Invalid request", content = @Content)
  @ApiResponse(responseCode = "404", description = "No such account for this user")
  @ApiResponse(
      responseCode = "409",
      description = "Declined for insufficient funds; the attempt is recorded")
  @ApiResponse(
      responseCode = "422",
      description = "Currency mismatch, or idempotency key used for a different payment")
  @ApiResponse(
      responseCode = "503",
      description = "Account busy with another payment; retry with the same key",
      headers = @Header(name = "Retry-After"))
  public ResponseEntity<?> submit(
      @PathVariable UUID userId,
      @PathVariable UUID accountId,
      @Parameter(description = "Client-generated key, unique per payment. A UUID is recommended.")
          @RequestHeader("Idempotency-Key")
          @NotBlank
          @Size(max = 255)
          String idempotencyKey,
      @Valid @RequestBody CreatePaymentRequest body) {
    if (!isKnownCurrency(body.amount().currency())) {
      return response(problem(HttpStatus.BAD_REQUEST, "validation_failed", "Unknown currency"));
    }

    PaymentResult result =
        service.submit(
            new PaymentRequest(
                userId,
                accountId,
                idempotencyKey,
                body.amount().value(),
                body.amount().currency(),
                body.beneficiary().name(),
                body.beneficiary().iban(),
                body.reference()));
    metrics.counter("payment.attempts", "outcome", result.outcome().code()).increment();

    return switch (result.outcome()) {
      case COMPLETED, INSUFFICIENT_FUNDS -> paymentResponse(userId, result.payment(), false);
        // Same status and body as the original, so a client retrying after a timeout sees the
        // response it missed.
      case REPLAYED -> paymentResponse(userId, result.payment(), true);
      case ACCOUNT_NOT_FOUND -> notFound(PaymentOutcome.ACCOUNT_NOT_FOUND.code(), "Account");
      case CURRENCY_MISMATCH ->
          response(
              problem(
                  HttpStatus.UNPROCESSABLE_ENTITY,
                  result.outcome().code(),
                  "Payment currency does not match the account currency"));
      case IDEMPOTENCY_KEY_REUSED ->
          response(
              problem(
                  HttpStatus.UNPROCESSABLE_ENTITY,
                  result.outcome().code(),
                  "Idempotency key already used for a different payment"));
    };
  }

  @GetMapping("/payments/{paymentId}")
  @Operation(summary = "Retrieve a payment, completed or declined")
  @ApiResponse(responseCode = "200")
  @ApiResponse(responseCode = "404", description = "No such payment on this user's account")
  public ResponseEntity<?> get(
      @PathVariable UUID userId, @PathVariable UUID accountId, @PathVariable UUID paymentId) {
    return service
        .payment(userId, accountId, paymentId)
        .<ResponseEntity<?>>map(payment -> ResponseEntity.ok(toResponse(payment)))
        .orElseGet(() -> notFound("payment_not_found", "Payment"));
  }

  @GetMapping("/balance")
  @Operation(summary = "Get the current balance")
  @ApiResponse(responseCode = "200")
  @ApiResponse(responseCode = "404", description = "No such account for this user")
  public ResponseEntity<?> balance(@PathVariable UUID userId, @PathVariable UUID accountId) {
    return service
        .account(userId, accountId)
        .<ResponseEntity<?>>map(
            account ->
                ResponseEntity.ok(
                    new BalanceResponse(
                        account.id(),
                        money(account.balance(), account.currency()),
                        account.updatedAt())))
        .orElseGet(() -> notFound(PaymentOutcome.ACCOUNT_NOT_FOUND.code(), "Account"));
  }

  /** 201 for a completed payment; 409 for a declined one, which still has an id and a location. */
  private ResponseEntity<?> paymentResponse(UUID userId, Payment payment, boolean replayed) {
    URI location =
        ServletUriComponentsBuilder.fromCurrentContextPath()
            .path("/api/v1/users/{userId}/accounts/{accountId}/payments/{paymentId}")
            .buildAndExpand(userId, payment.accountId(), payment.id())
            .toUri();
    Object body;
    if (payment.isCompleted()) {
      body = toResponse(payment);
    } else {
      ProblemDetail problem =
          problem(
              HttpStatus.CONFLICT,
              PaymentOutcome.INSUFFICIENT_FUNDS.code(),
              "The account balance does not cover this payment");
      problem.setProperty("paymentId", payment.id());
      body = problem;
    }
    ResponseEntity.BodyBuilder response =
        ResponseEntity.status(payment.isCompleted() ? HttpStatus.CREATED : HttpStatus.CONFLICT)
            .location(location);
    if (replayed) {
      response.header(REPLAYED_HEADER, "true");
    }
    return response.body(body);
  }

  private static ResponseEntity<?> notFound(String code, String what) {
    return response(problem(HttpStatus.NOT_FOUND, code, what + " not found"));
  }

  private static ResponseEntity<?> response(ProblemDetail problem) {
    return ResponseEntity.status(problem.getStatus()).body(problem);
  }

  private static PaymentResponse toResponse(Payment payment) {
    return new PaymentResponse(
        payment.id(),
        payment.accountId(),
        payment.status().name(),
        money(payment.amount(), payment.currency()),
        new BeneficiaryDto(payment.beneficiaryName(), payment.beneficiaryIban()),
        payment.reference(),
        payment.failureReason(),
        payment.createdAt());
  }

  /** Renders at the currency's precision (250.5000 CHF as 250.50), never dropping a digit. */
  private static MoneyDto money(BigDecimal amount, String currency) {
    int digits = Currency.getInstance(currency).getDefaultFractionDigits();
    int scale = Math.max(Math.max(digits, 0), amount.stripTrailingZeros().scale());
    return new MoneyDto(amount.setScale(scale), currency);
  }

  private static boolean isKnownCurrency(String code) {
    try {
      Currency.getInstance(code);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
