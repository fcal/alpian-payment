package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreatePaymentRequest(
    @NotNull @Valid MoneyDto amount,
    @NotNull @Valid BeneficiaryDto beneficiary,
    @Schema(example = "Invoice 42") @Size(max = 140) String reference) {}
