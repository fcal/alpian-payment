package com.alpian.payment.api;

import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.api.dto.PaymentResponse;
import com.alpian.payment.domain.IdempotencyKey;
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
import java.util.UUID;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

/**
 * The OpenAPI documentation of {@link PaymentController}, kept apart so the controller reads as
 * behaviour. Validation constraints live here too: an implementation may not add parameter
 * constraints to a method it overrides.
 */
@Tag(name = "Payments", description = "Outbound payments from an account")
interface PaymentApi {

  /** Set on a response that replays an earlier outcome rather than executing anything. */
  String REPLAYED_HEADER = "Idempotent-Replayed";

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
  ResponseEntity<?> submit(
      UUID userId,
      UUID accountId,
      @Parameter(
              in = ParameterIn.HEADER,
              description =
                  "Client-generated key identifying this payment, unique per account. A UUID is"
                      + " recommended.",
              required = true,
              example = "3f1c2b9e-5d4a-4c8e-9f7a-1b2c3d4e5f60")
          @NotBlank
          @Size(max = IdempotencyKey.MAX_LENGTH)
          String idempotencyKey,
      @Valid CreatePaymentRequest body);

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
  ResponseEntity<?> get(UUID userId, UUID accountId, UUID paymentId);
}
