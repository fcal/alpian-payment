package com.alpian.payment.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

/** Serialised as a decimal string, so clients never parse money into a binary float. */
public record MoneyDto(
    @Schema(type = "string", example = "250.50", description = "At most 4 decimal places")
        @NotNull
        @DecimalMin(value = "0", inclusive = false)
        @Digits(integer = 15, fraction = 4)
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        BigDecimal value,
    @Schema(example = "CHF", description = "ISO 4217 code") @NotNull @Pattern(regexp = "[A-Z]{3}")
        String currency) {}
