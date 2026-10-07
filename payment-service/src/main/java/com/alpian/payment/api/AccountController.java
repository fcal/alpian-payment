package com.alpian.payment.api;

import com.alpian.payment.api.dto.BalanceResponse;
import com.alpian.payment.domain.AccountId;
import com.alpian.payment.domain.UserId;
import com.alpian.payment.service.AccountService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Account queries.
 *
 * <p>As with {@link PaymentController}, {@code userId} in the path stands in for an authenticated
 * principal; in production it must come from a validated JWT, not from the URL.
 */
@RestController
@RequestMapping("/api/v1/users/{userId}/accounts/{accountId}")
@Tag(name = "Accounts", description = "Account balances")
public class AccountController {

  private final AccountService accountService;

  public AccountController(AccountService accountService) {
    this.accountService = accountService;
  }

  @GetMapping("/balance")
  @Operation(
      summary = "Get the current balance",
      description =
          "A snapshot, not a reservation: a payment submitted afterwards is checked against the"
              + " balance at the moment it executes, under the account lock.")
  @ApiResponse(
      responseCode = "200",
      content = @Content(schema = @Schema(implementation = BalanceResponse.class)))
  @ApiResponse(
      responseCode = "404",
      description = "No such account for this user",
      content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
  public ResponseEntity<?> balance(@PathVariable UUID userId, @PathVariable UUID accountId) {
    return accountService
        .ownedAccount(new UserId(userId), new AccountId(accountId))
        .<ResponseEntity<?>>map(account -> ResponseEntity.ok(ApiMapper.toBalanceResponse(account)))
        .orElseGet(
            () -> {
              ProblemDetail problem =
                  Problems.of(HttpStatus.NOT_FOUND, "account_not_found", "Account not found");
              return ResponseEntity.status(HttpStatus.NOT_FOUND).body(problem);
            });
  }
}
