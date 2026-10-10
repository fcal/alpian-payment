package com.alpian.payment.api;

import com.alpian.payment.api.dto.CreatePaymentRequest;
import com.alpian.payment.api.dto.PaymentResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.UUID;
import org.springframework.http.ResponseEntity;

/**
 * The OpenAPI documentation of {@link PaymentController}, kept apart so the controller reads as
 * behaviour. Validation constraints live here too: an implementation may not add parameter
 * constraints to a method it overrides.
 */
interface PaymentApi {

  String REPLAYED_HEADER = "Idempotent-Replayed";

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
  ResponseEntity<?> submit(
      UUID userId,
      UUID accountId,
      @Parameter(description = "Client-generated key, unique per payment. A UUID is recommended.")
          @NotBlank
          @Size(max = 255)
          String idempotencyKey,
      @Valid CreatePaymentRequest body);

  @Operation(summary = "Retrieve a payment, completed or declined")
  @ApiResponse(responseCode = "200")
  @ApiResponse(responseCode = "404", description = "No such payment on this user's account")
  ResponseEntity<?> get(UUID userId, UUID accountId, UUID paymentId);

  @Operation(summary = "Get the current balance")
  @ApiResponse(responseCode = "200")
  @ApiResponse(responseCode = "404", description = "No such account for this user")
  ResponseEntity<?> balance(UUID userId, UUID accountId);
}
