package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.UUID;

@Schema(description = "The current balance of an account")
public record BalanceResponse(
    UUID accountId,
    MoneyDto balance,
    @Schema(description = "When the balance last changed") Instant asOf) {}
