package com.alpian.payment.api;

import com.alpian.payment.domain.PaymentOutcome;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

/**
 * Builds RFC 9457 problem responses.
 *
 * <p>Every error carries a {@code code} property holding the {@link PaymentOutcome} tag, the same
 * value used in metrics. Clients should branch on {@code code} rather than parse {@code detail},
 * which is prose for humans and may be reworded.
 */
final class Problems {

  private Problems() {}

  static ProblemDetail of(HttpStatus status, PaymentOutcome outcome, String detail) {
    return of(status, outcome.tagValue(), detail);
  }

  static ProblemDetail of(HttpStatus status, String code, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
    // Relative URI reference, which RFC 9457 permits. Resolves against the API's own origin, so
    // it identifies the problem type without hard-coding a host.
    problem.setType(URI.create("/problems/" + code.replace('_', '-')));
    problem.setTitle(titleFor(status, code));
    problem.setProperty("code", code);
    return problem;
  }

  private static String titleFor(HttpStatus status, String code) {
    return switch (code) {
      case "insufficient_funds" -> "Insufficient funds";
      case "currency_mismatch" -> "Currency mismatch";
      case "account_not_found", "account_not_owned" -> "Account not found";
      case "idempotency_key_reused" -> "Idempotency key reused";
      case "lock_timeout" -> "Account busy";
      case "validation_failed" -> "Invalid request";
      default -> status.getReasonPhrase();
    };
  }
}
