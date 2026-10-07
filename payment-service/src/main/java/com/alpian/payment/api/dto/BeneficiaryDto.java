package com.alpian.payment.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Schema(description = "The external party receiving the payment")
public record BeneficiaryDto(
    @Schema(example = "Acme GmbH") @NotBlank @Size(max = 140) String name,
    @Schema(
            description = "IBAN. Spaces are ignored. Checked for shape only, not checksum.",
            example = "CH93 0076 2011 6238 5295 7")
        @NotBlank
        // 34 characters is the IBAN maximum; allow for grouping spaces a client may include.
        @Size(max = 42)
        String iban) {}
