package com.alpian.notification.streams;

import com.alpian.notification.config.NotificationProperties;
import com.alpian.notification.observability.NotificationMetrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

/**
 * The deduplication topology, built with the Processor API.
 *
 * <pre>
 *   payment-events ──▶ deduplicate ──▶ notification-events
 *                          │  ▲
 *                          │  └── notified-payments store (changelogged)
 *                          └────▶ payment-events-dlt
 * </pre>
 *
 * <p>The Processor API rather than the DSL, because one processor routes to two sinks by name, and
 * owns the store and the punctuator that bounds it.
 *
 * <p>Values are read and written as raw bytes, and decoded inside the processor. A record that does
 * not decode then reaches the dead-letter topic with its original bytes intact; a protobuf {@code
 * Serde} would fail in the deserializer, before any processor could route it.
 */
public final class DeduplicationTopology {

  public static final String STORE = "notified-payments";

  static final String SOURCE = "payment-events-source";
  static final String PROCESSOR = "deduplicate";
  static final String NOTIFICATIONS_SINK = "notifications-sink";
  static final String DEAD_LETTER_SINK = "dead-letter-sink";

  private DeduplicationTopology() {}

  /** Adds the deduplication nodes to {@code topology} and returns it. */
  public static Topology addTo(
      Topology topology, NotificationProperties properties, NotificationMetrics metrics) {
    topology.addSource(
        SOURCE,
        Serdes.String().deserializer(),
        Serdes.ByteArray().deserializer(),
        properties.inputTopic());
    topology.addProcessor(
        PROCESSOR,
        () ->
            new DeduplicationProcessor(
                STORE, properties.deduplicationRetention(), properties.purgeInterval(), metrics),
        SOURCE);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.Long()),
        PROCESSOR);
    topology.addSink(
        NOTIFICATIONS_SINK,
        properties.outputTopic(),
        Serdes.String().serializer(),
        Serdes.ByteArray().serializer(),
        PROCESSOR);
    topology.addSink(
        DEAD_LETTER_SINK,
        properties.deadLetterTopic(),
        Serdes.String().serializer(),
        Serdes.ByteArray().serializer(),
        PROCESSOR);
    return topology;
  }
}
