package com.alpian.payment.api;

import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.Payment;
import com.alpian.payment.domain.PaymentId;
import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.domain.PaymentResult;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.service.AccountService;
import com.alpian.payment.service.PaymentService;
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
 *
 * <p>Documented in {@link PaymentApi}.
 */
@RestController
@RequestMapping("/api/v1/users/{userId}/accounts/{accountId}/payments")
public class PaymentController implements PaymentApi {

  private final PaymentService paymentService;
  private final AccountService accountService;

  public PaymentController(PaymentService paymentService, AccountService accountService) {
    this.paymentService = paymentService;
    this.accountService = accountService;
  }

  @Override
  @PostMapping
  public ResponseEntity<?> submit(
      @PathVariable UUID userId,
      @PathVariable UUID accountId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody CreatePaymentRequest body) {
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

  @Override
  @GetMapping("/{paymentId}")
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
