package com.alpian.payment.component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

/** The payment service's REST API, as an external client sees it. */
final class PaymentApi {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final String baseUrl;

  PaymentApi(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  /** A response with its parsed body. */
  record Response(int status, JsonNode body, HttpResponse<String> raw) {

    String paymentId() {
      return body.get("paymentId").asText();
    }

    boolean replayed() {
      return raw.headers().firstValue("Idempotent-Replayed").map("true"::equals).orElse(false);
    }
  }

  Response pay(UUID user, UUID account, String idempotencyKey, String amount, String currency) {
    String body =
        """
        {"amount":{"value":"%s","currency":"%s"},
         "beneficiary":{"name":"Acme GmbH","iban":"CH93 0076 2011 6238 5295 7"},
         "reference":"Component test"}
        """
            .formatted(amount, currency);
    return send(
        HttpRequest.newBuilder(uri(user, account, "/payments"))
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", idempotencyKey)
            .POST(HttpRequest.BodyPublishers.ofString(body)));
  }

  Response balance(UUID user, UUID account) {
    return send(HttpRequest.newBuilder(uri(user, account, "/balance")).GET());
  }

  private URI uri(UUID user, UUID account, String path) {
    return URI.create(baseUrl + "/api/v1/users/" + user + "/accounts/" + account + path);
  }

  private Response send(HttpRequest.Builder request) {
    try {
      HttpResponse<String> response =
          http.send(
              request.timeout(Duration.ofSeconds(10)).build(),
              HttpResponse.BodyHandlers.ofString());
      return new Response(response.statusCode(), JSON.readTree(response.body()), response);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
