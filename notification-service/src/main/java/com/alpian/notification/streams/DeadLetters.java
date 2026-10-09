package com.alpian.notification.streams;

/**
 * Headers added to a dead-lettered payment event. The original value, key and headers are kept
 * unchanged alongside them, so the record can be inspected and replayed as it was received.
 */
public final class DeadLetters {

  /** {@link DeadLetterReason#code()}. */
  public static final String REASON = "dlt-reason";

  /** Human-readable detail of what was wrong. */
  public static final String ERROR = "dlt-error";

  public static final String ORIGINAL_TOPIC = "dlt-original-topic";
  public static final String ORIGINAL_PARTITION = "dlt-original-partition";
  public static final String ORIGINAL_OFFSET = "dlt-original-offset";

  private DeadLetters() {}
}
