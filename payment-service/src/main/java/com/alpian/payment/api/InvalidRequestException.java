package com.alpian.payment.api;

/**
 * A request that passed shape validation but could not be converted into domain types — an unknown
 * currency code that still matches {@code [A-Z]{3}}, for instance.
 *
 * <p>A dedicated type rather than catching {@link IllegalArgumentException} globally. That would
 * turn every {@code IllegalArgumentException} anywhere in the application into a {@code 400},
 * including ones thrown by genuine bugs, which would then be reported as the client's fault and
 * never investigated.
 */
public class InvalidRequestException extends RuntimeException {

  public InvalidRequestException(String message, Throwable cause) {
    super(message, cause);
  }
}
