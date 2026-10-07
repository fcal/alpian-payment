package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Request body for submitting a payment.
 *
 * <p>Separate from the service's {@code PaymentRequest}: this type is the public wire contract and
 * changes only for client compatibility, whereas the service type changes with business rules.
 * Keeping them apart means a field rename on one side is not forced on the other. Validation here
 * covers shape; whether the payment is permissible is the service's decision.
 */
@Schema(description = "An outbound payment from the account in the path")
public record CreatePaymentRequest(
    @NotNull @Valid MoneyDto amount,
    @NotNull @Valid BeneficiaryDto beneficiary,
    @Schema(description = "Free-text reference shown to the beneficiary", example = "Invoice 42")
        @Size(max = 140)
        String reference) {}
