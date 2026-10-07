package com.alpian.payment.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

/**
 * A monetary amount on the wire.
 *
 * <p>The value is a decimal string in responses, never a JSON number. JSON numbers are commonly
 * parsed into binary floating point by clients, which cannot represent most decimal fractions
 * exactly — {@code 0.1 + 0.2} is not {@code 0.3} in JavaScript. A string forces the client to make
 * a deliberate choice of decimal type. Requests accept either form, since Jackson parses a JSON
 * number's text directly into {@link BigDecimal} without passing through a double.
 */
@Schema(description = "A monetary amount in a single currency")
public record MoneyDto(
    @Schema(
            description =
                "Decimal amount, at most 4 fractional digits. A string in responses; requests"
                    + " accept a string or a number.",
            example = "250.50",
            type = "string")
        @NotNull
        @DecimalMin(value = "0", inclusive = false, message = "must be greater than zero")
        // 19 digits in total with 4 after the point, matching the NUMERIC(19,4) column.
        @Digits(integer = 15, fraction = 4)
        // Serialised as a string. Without this Jackson writes BigDecimal as a JSON number, which
        // defeats the point: the client's parser would turn it into a double.
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        BigDecimal value,
    @Schema(description = "ISO 4217 alphabetic currency code", example = "CHF")
        @NotBlank
        @Pattern(regexp = "[A-Z]{3}", message = "must be a three-letter ISO 4217 code")
        String currency) {}
