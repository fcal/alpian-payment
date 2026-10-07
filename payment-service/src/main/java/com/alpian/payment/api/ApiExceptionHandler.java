package com.alpian.payment.api;

import com.alpian.payment.domain.PaymentOutcome;
import com.alpian.payment.observability.PaymentMetrics;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Renders every error as an RFC 9457 problem.
 *
 * <p>Business outcomes — insufficient funds, unknown account — are not exceptions and are mapped by
 * the controller. This class handles what is left: malformed requests, which are the client's
 * fault, and unexpected failures, which are ours. The two must stay distinguishable, which is why
 * there is no catch-all for {@link IllegalArgumentException}.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  private final PaymentMetrics metrics;

  public ApiExceptionHandler(PaymentMetrics metrics) {
    this.metrics = metrics;
  }

  /** Body validation failures, with a per-field breakdown so a client can show them in place. */
  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    List<Map<String, String>> errors =
        ex.getBindingResult().getFieldErrors().stream()
            .map(
                e ->
                    Map.of(
                        "field", e.getField(),
                        "message", String.valueOf(e.getDefaultMessage())))
            .toList();
    ProblemDetail problem = validationProblem("Request body is invalid");
    problem.setProperty("errors", errors);
    return handleExceptionInternal(ex, problem, headers, HttpStatus.BAD_REQUEST, request);
  }

  /** Header and path-variable constraint failures, such as an over-long idempotency key. */
  @Override
  protected ResponseEntity<Object> handleHandlerMethodValidationException(
      HandlerMethodValidationException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    List<Map<String, String>> errors =
        ex.getAllValidationResults().stream()
            .flatMap(
                result ->
                    result.getResolvableErrors().stream()
                        .map(
                            error ->
                                Map.of(
                                    "parameter",
                                    String.valueOf(result.getMethodParameter().getParameterName()),
                                    "message",
                                    String.valueOf(error.getDefaultMessage()))))
            .toList();
    ProblemDetail problem = validationProblem("Request parameters are invalid");
    problem.setProperty("errors", errors);
    return handleExceptionInternal(ex, problem, headers, HttpStatus.BAD_REQUEST, request);
  }

  @ExceptionHandler(InvalidRequestException.class)
  ResponseEntity<Object> handleInvalidRequest(InvalidRequestException ex, WebRequest request) {
    return handleExceptionInternal(
        ex, validationProblem(ex.getMessage()), new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
  }

  /**
   * Anything unforeseen. Logged with its stack trace, answered with a generic message: the detail
   * of an internal failure is for the log, not for the client, and can disclose implementation.
   */
  @ExceptionHandler(Exception.class)
  ResponseEntity<Object> handleUnexpected(Exception ex, WebRequest request) {
    log.error("Unhandled exception processing {}", describe(request), ex);
    ProblemDetail problem =
        Problems.of(
            HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "An unexpected error occurred");
    return handleExceptionInternal(
        ex, problem, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
  }

  /**
   * Every framework-level 4xx passes through here — malformed JSON, a missing {@code
   * Idempotency-Key}, a path segment that is not a UUID — so it is the one place a rejected payment
   * submission can be counted. Without this, requests that never reached the service would be
   * absent from {@code payment_attempts_total}, and a client sending broken requests would be
   * invisible in the metrics.
   */
  @Override
  protected ResponseEntity<Object> handleExceptionInternal(
      Exception ex,
      @Nullable Object body,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    if (status.is4xxClientError() && isPaymentSubmission(request)) {
      metrics.recordAttempt(PaymentOutcome.VALIDATION_FAILED);
    }

    // The body is enriched after super has run, not before. For most framework exceptions the
    // base class passes body = null and builds the ProblemDetail inside this very call, so
    // inspecting `body` beforehand finds nothing to add to.
    ResponseEntity<Object> response =
        super.handleExceptionInternal(ex, body, headers, status, request);
    if (response != null
        && response.getBody() instanceof ProblemDetail problem
        && (problem.getProperties() == null || !problem.getProperties().containsKey("code"))) {
      // Framework problems carry no `code`; adding one lets clients branch on every error the
      // same way, whichever layer produced it.
      problem.setProperty(
          "code",
          status.is4xxClientError()
              ? PaymentOutcome.VALIDATION_FAILED.tagValue()
              : "internal_error");
    }
    return response;
  }

  private static ProblemDetail validationProblem(String detail) {
    return Problems.of(HttpStatus.BAD_REQUEST, PaymentOutcome.VALIDATION_FAILED, detail);
  }

  private static boolean isPaymentSubmission(WebRequest request) {
    HttpServletRequest servlet =
        request instanceof NativeWebRequest nativeRequest
            ? nativeRequest.getNativeRequest(HttpServletRequest.class)
            : null;
    return servlet != null
        && "POST".equals(servlet.getMethod())
        && servlet.getRequestURI().endsWith("/payments");
  }

  private static String describe(WebRequest request) {
    return request.getDescription(false);
  }
}
