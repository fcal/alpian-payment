package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.UUID;

@Schema(description = "A recorded payment attempt")
public record PaymentResponse(
    @Schema(description = "Identifier of the payment, also carried on its notification event")
        UUID paymentId,
    UUID accountId,
    @Schema(description = "COMPLETED when funds left the account, FAILED when declined")
        String status,
    MoneyDto amount,
    BeneficiaryDto beneficiary,
    String reference,
    @Schema(description = "Present only when status is FAILED", example = "Insufficient funds")
        String failureReason,
    Instant createdAt) {}
