package com.alpian.notification;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * payment-events → deduplicate (notified-payments store) → notification-events.
 *
 * <p>Values are read as raw bytes and decoded in the processor, so a bad record can be logged and
 * skipped instead of failing the deserializer.
 */
public final class DeduplicationTopology {

  public static final String INPUT_TOPIC = "payment-events";
  public static final String OUTPUT_TOPIC = "notification-events";
  public static final String STORE = "notified-payments";

  private DeduplicationTopology() {}

  public static KStream<String, byte[]> build(StreamsBuilder builder, MeterRegistry metrics) {
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.Long()));
    KStream<String, byte[]> notifications =
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.ByteArray()))
            .process(() -> new DeduplicationProcessor(metrics), STORE);
    notifications.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.ByteArray()));
    return notifications;
  }
}
