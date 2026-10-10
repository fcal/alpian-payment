package com.alpian.payment.api.dto;

import java.time.Instant;
import java.util.UUID;

public record BalanceResponse(UUID accountId, MoneyDto balance, Instant asOf) {}
