package com.alpian.notification;

import com.alpian.payment.events.v1.NotificationEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Delivers the deduplicated notifications. Kept outside the Streams topology, because a side effect
 * inside it would be repeated when an exactly-once transaction is retried. Reads with {@code
 * read_committed}; failures are retried, then dead-lettered (see {@link KafkaConfiguration}).
 */
@Component
class NotificationListener {

  private final NotificationSender sender;

  NotificationListener(NotificationSender sender) {
    this.sender = sender;
  }

  @KafkaListener(topics = DeduplicationTopology.OUTPUT_TOPIC)
  void deliver(ConsumerRecord<String, byte[]> record) throws InvalidProtocolBufferException {
    sender.send(NotificationEvent.parseFrom(record.value()));
  }
}
