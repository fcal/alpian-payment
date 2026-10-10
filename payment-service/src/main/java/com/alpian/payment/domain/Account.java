package com.alpian.payment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Account(
    UUID id, UUID userId, BigDecimal balance, String currency, Instant updatedAt) {}
