package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(
    UUID paymentId,
    UUID accountId,
    @Schema(allowableValues = {"COMPLETED", "FAILED"}) String status,
    MoneyDto amount,
    BeneficiaryDto beneficiary,
    String reference,
    @Schema(description = "Present when status is FAILED") String failureReason,
    Instant createdAt) {}
