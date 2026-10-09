package com.alpian.notification.delivery;

import com.alpian.notification.observability.NotificationMetrics;
import com.alpian.notification.observability.NotificationMetrics.DeliveryOutcome;
import com.alpian.payment.events.v1.NotificationEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Delivers the deduplicated notifications.
 *
 * <p>Delivery is deliberately outside the Streams topology. Under exactly-once, an aborted Streams
 * transaction is re-run, so a side effect inside a processor — an email sent — would happen again,
 * outside any transaction's reach. Here, the topology's output is read with {@code read_committed},
 * so only notifications from committed transactions are seen, each exactly once on the topic.
 *
 * <p>Delivery itself is at-least-once: a crash after sending and before the offset commit sends
 * again on restart. That is the residual duplicate no Kafka guarantee can remove, and the reason
 * {@link NotificationSender} asks for an idempotency key at the provider.
 *
 * <p>Failures are retried with backoff, then dead-lettered; see {@link DeliveryConfiguration}.
 */
@Component
class NotificationListener {

  private final NotificationSender sender;
  private final NotificationMetrics metrics;

  NotificationListener(NotificationSender sender, NotificationMetrics metrics) {
    this.sender = sender;
    this.metrics = metrics;
  }

  @KafkaListener(topics = "${notification.output-topic}")
  void deliver(ConsumerRecord<String, byte[]> record) throws InvalidProtocolBufferException {
    NotificationEvent notification;
    try {
      notification = NotificationEvent.parseFrom(record.value());
    } catch (InvalidProtocolBufferException e) {
      metrics.recordDelivery(DeliveryOutcome.FAILED);
      throw e;
    }
    try {
      sender.send(notification);
    } catch (RuntimeException e) {
      metrics.recordDelivery(DeliveryOutcome.FAILED);
      throw e;
    }
    metrics.recordDelivery(DeliveryOutcome.SENT);
  }
}
