package com.alpian.payment.api;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Renders every error as an RFC 9457 problem with a {@code code} property. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  private final MeterRegistry metrics;

  public ApiExceptionHandler(MeterRegistry metrics) {
    this.metrics = metrics;
  }

  static ProblemDetail problem(HttpStatus status, String code, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
    problem.setProperty("code", code);
    return problem;
  }

  /** Another payment holds the account lock. Nothing changed, so the client can safely retry. */
  @ExceptionHandler(CannotAcquireLockException.class)
  ResponseEntity<ProblemDetail> lockTimeout() {
    metrics.counter("payment.attempts", "outcome", "lock_timeout").increment();
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, "1")
        .body(
            problem(
                HttpStatus.SERVICE_UNAVAILABLE,
                "lock_timeout",
                "Another payment on this account is in progress"));
  }

  /** Internal details go to the log, not to the client. */
  @ExceptionHandler(Exception.class)
  ResponseEntity<ProblemDetail> unexpected(Exception ex, WebRequest request) {
    log.error("Unhandled exception processing {}", request.getDescription(false), ex);
    return ResponseEntity.internalServerError()
        .body(
            problem(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "An unexpected error occurred"));
  }

  /** Lists what is invalid, so a client can show it in place. */
  @Override
  protected ResponseEntity<Object> handleHandlerMethodValidationException(
      HandlerMethodValidationException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    List<String> errors =
        ex.getAllErrors().stream()
            .map(
                e -> (e instanceof FieldError f ? f.getField() + ": " : "") + e.getDefaultMessage())
            .toList();
    ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "validation_failed", "Invalid request");
    problem.setProperty("errors", errors);
    return handleExceptionInternal(ex, problem, headers, HttpStatus.BAD_REQUEST, request);
  }

  /** Adds a {@code code} to the problems Spring builds itself (malformed JSON, missing header). */
  @Override
  protected ResponseEntity<Object> handleExceptionInternal(
      Exception ex, Object body, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
    ResponseEntity<Object> response =
        super.handleExceptionInternal(ex, body, headers, status, request);
    if (response != null && response.getBody() instanceof ProblemDetail problem) {
      if (problem.getProperties() == null || !problem.getProperties().containsKey("code")) {
        problem.setProperty(
            "code", status.is4xxClientError() ? "validation_failed" : "internal_error");
      }
    }
    return response;
  }
}
