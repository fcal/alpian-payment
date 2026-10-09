package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record BeneficiaryDto(
    @Schema(example = "Acme GmbH") @NotBlank @Size(max = 140) String name,
    @Schema(example = "CH93 0076 2011 6238 5295 7", description = "Spaces are ignored")
        @NotBlank
        @Size(max = 42)
        String iban) {}
