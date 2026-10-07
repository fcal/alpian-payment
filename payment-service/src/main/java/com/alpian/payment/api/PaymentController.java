package com.alpian.payment.api;

import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.api.dto.PaymentResponse;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.IdempotencyKey;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.service.AccountService;
import com.alpian.payment.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
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
 * Submits and retrieves payments.
 *
 * <p><strong>The {@code userId} path segment is a stand-in for authentication.</strong> In
 * production the user must come from the subject of a validated JWT, never from the URL: as
 * written, anyone can claim to be anyone. It is in the path only because authentication is out of
 * scope for this exercise. The ownership check behind it is real, though — an account is only ever
 * acted on for its owner — so replacing the path variable with the authenticated principal is the
 * whole of the change needed.
 */
@RestController
@RequestMapping("/api/v1/users/{userId}/accounts/{accountId}/payments")
@Tag(name = "Payments", description = "Outbound payments from an account")
public class PaymentController {

  /** Set on a response that replays an earlier outcome rather than executing anything. */
  static final String REPLAYED_HEADER = "Idempotent-Replayed";

  private final PaymentService paymentService;
  private final AccountService accountService;

  public PaymentController(PaymentService paymentService, AccountService accountService) {
    this.paymentService = paymentService;
    this.accountService = accountService;
  }

  @PostMapping
  @Operation(
      summary = "Submit an outbound payment",
      description =
          """
          Debits the account and records the payment, or declines it. Submission is idempotent: \
          resending a request with the same `Idempotency-Key` returns the original outcome, \
          including the original status code, with `Idempotent-Replayed: true`, and never debits \
          twice. A key identifies one attempt — replaying the key of a declined payment returns \
          that decline even if the account has since been funded. Reusing a key for a different \
          payment is refused with 422.""")
  @ApiResponse(
      responseCode = "201",
      description = "Payment completed",
      headers = {
        @Header(name = "Location", description = "The created payment"),
        @Header(name = REPLAYED_HEADER, description = "Present and true on a replay")
      },
      content = @Content(schema = @Schema(implementation = PaymentResponse.class)))
  @ApiResponse(
      responseCode = "400",
      description = "Malformed request or invalid field",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  @ApiResponse(
      responseCode = "404",
      description = "No such account for this user",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  @ApiResponse(
      responseCode = "409",
      description =
          "Declined for insufficient funds. The attempt is recorded; `paymentId` and"
              + " `Location` identify it.",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  @ApiResponse(
      responseCode = "422",
      description =
          "Currency differs from the account's, or the idempotency key was used for a"
              + " different payment",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  @ApiResponse(
      responseCode = "503",
      description =
          "Another payment on this account is in progress. Nothing was changed; retry after"
              + " `Retry-After` seconds with the same key.",
      headers = @Header(name = "Retry-After"),
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  public ResponseEntity<?> submit(
      @PathVariable UUID userId,
      @PathVariable UUID accountId,
      @Parameter(
              in = ParameterIn.HEADER,
              description =
                  "Client-generated key identifying this payment, unique per account. A UUID is"
                      + " recommended.",
              required = true,
              example = "3f1c2b9e-5d4a-4c8e-9f7a-1b2c3d4e5f60")
          @RequestHeader("Idempotency-Key")
          @NotBlank
          @Size(max = IdempotencyKey.MAX_LENGTH)
          String idempotencyKey,
      @Valid @RequestBody CreatePaymentRequest body) {

    PaymentResult result =
        paymentService.submit(
            ApiMapper.toPaymentRequest(
                new UserId(userId), new AccountId(accountId), idempotencyKey, body));

    return switch (result) {
      case PaymentResult.Completed completed -> created(userId, completed.payment(), false);
      case PaymentResult.Declined declined -> declined(userId, declined.attempt(), false);
      case PaymentResult.Replayed replayed ->
          // A replay returns what the first request returned, status code included, so a client
          // retrying after a timeout sees exactly the response it missed.
          replayed.original().isCompleted()
              ? created(userId, replayed.original(), true)
              : declined(userId, replayed.original(), true);
      case PaymentResult.Rejected rejected -> rejected(rejected);
    };
  }

  @GetMapping("/{paymentId}")
  @Operation(
      summary = "Retrieve a payment",
      description =
          "Resolves the outcome of a submission, for instance after a client timeout. Returns"
              + " declined attempts as well as completed ones.")
  @ApiResponse(
      responseCode = "200",
      content = @Content(schema = @Schema(implementation = PaymentResponse.class)))
  @ApiResponse(
      responseCode = "404",
      description = "No such payment on this user's account",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  public ResponseEntity<?> get(
      @PathVariable UUID userId, @PathVariable UUID accountId, @PathVariable UUID paymentId) {
    return accountService
        .payment(new UserId(userId), new AccountId(accountId), new PaymentId(paymentId))
        .<ResponseEntity<?>>map(payment -> ResponseEntity.ok(ApiMapper.toResponse(payment)))
        .orElseGet(
            () ->
                problem(
                    Problems.of(HttpStatus.NOT_FOUND, "payment_not_found", "Payment not found")));
  }

  private ResponseEntity<?> created(UUID userId, Payment payment, boolean replayed) {
    ResponseEntity.BodyBuilder response = ResponseEntity.created(locationOf(userId, payment));
    if (replayed) {
      response.header(REPLAYED_HEADER, "true");
    }
    return response.body(ApiMapper.toResponse(payment));
  }

  /**
   * A decline is an error to the caller but a recorded fact to the system, so the problem carries
   * the payment's id and a {@code Location} for it: the attempt exists and can be retrieved.
   */
  private ResponseEntity<?> declined(UUID userId, Payment payment, boolean replayed) {
    ProblemDetail problem =
        Problems.of(
            HttpStatus.CONFLICT,
            PaymentOutcome.INSUFFICIENT_FUNDS,
            "The account balance does not cover this payment");
    problem.setProperty("paymentId", payment.id().value());

    HttpHeaders headers = new HttpHeaders();
    headers.setLocation(locationOf(userId, payment));
    if (replayed) {
      headers.set(REPLAYED_HEADER, "true");
    }
    return ResponseEntity.status(HttpStatus.CONFLICT).headers(headers).body(problem);
  }

  private ResponseEntity<?> rejected(PaymentResult.Rejected rejected) {
    return switch (rejected.outcome()) {
        // Deliberately identical. The response must not reveal that an account exists but belongs
        // to someone else; the distinction lives in the metric and the warn log.
      case ACCOUNT_NOT_FOUND, ACCOUNT_NOT_OWNED ->
          problem(
              Problems.of(
                  HttpStatus.NOT_FOUND, PaymentOutcome.ACCOUNT_NOT_FOUND, rejected.detail()));

      case CURRENCY_MISMATCH, IDEMPOTENCY_KEY_REUSED ->
          problem(
              Problems.of(HttpStatus.UNPROCESSABLE_ENTITY, rejected.outcome(), rejected.detail()));

      case LOCK_TIMEOUT ->
          // Retry-After makes the transient nature machine-readable. One second comfortably
          // exceeds the transaction time of the payment holding the lock.
          ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
              .header(HttpHeaders.RETRY_AFTER, "1")
              .body(
                  Problems.of(
                      HttpStatus.SERVICE_UNAVAILABLE, rejected.outcome(), rejected.detail()));

      case VALIDATION_FAILED ->
          problem(Problems.of(HttpStatus.BAD_REQUEST, rejected.outcome(), rejected.detail()));

        // Not rejections: the sealed hierarchy routes these through other variants, and
        // PaymentResult.Rejected refuses the two that produce journal entries.
      case COMPLETED, INSUFFICIENT_FUNDS, IDEMPOTENT_REPLAY ->
          throw new IllegalStateException("Not a rejection outcome: " + rejected.outcome());
    };
  }

  private static ResponseEntity<?> problem(ProblemDetail problem) {
    return ResponseEntity.status(problem.getStatus()).body(problem);
  }

  private static URI locationOf(UUID userId, Payment payment) {
    return ServletUriComponentsBuilder.fromCurrentContextPath()
        .path("/api/v1/users/{userId}/accounts/{accountId}/payments/{paymentId}")
        .buildAndExpand(userId, payment.accountId().value(), payment.id().value())
        .toUri();
  }
}
