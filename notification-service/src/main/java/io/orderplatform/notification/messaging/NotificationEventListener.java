package io.orderplatform.notification.messaging;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.notification.config.NotificationKafkaConfig;
import io.orderplatform.notification.domain.CustomerNotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventListener {

    private final CustomerNotificationService notificationService;
    private final Tracer tracer;

    public NotificationEventListener(CustomerNotificationService notificationService, Tracer tracer) {
        this.notificationService = notificationService;
        this.tracer = tracer;
    }

    @KafkaListener(
            topics = TopicNames.ORDER_CONFIRMED,
            groupId = NotificationKafkaConfig.CONSUMER_GROUP,
            containerFactory = "notificationKafkaListenerContainerFactory")
    public void onOrderConfirmed(ConsumerRecord<String, OrderConfirmed> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "notification-service onOrderConfirmed", record, () -> {
            notificationService.onOrderConfirmed(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(
            topics = TopicNames.ORDER_CANCELLED,
            groupId = NotificationKafkaConfig.CONSUMER_GROUP,
            containerFactory = "notificationKafkaListenerContainerFactory")
    public void onOrderCancelled(ConsumerRecord<String, OrderCancelled> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "notification-service onOrderCancelled", record, () -> {
            notificationService.onOrderCancelled(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(
            topics = TopicNames.PAYMENT_REFUNDED,
            groupId = NotificationKafkaConfig.CONSUMER_GROUP,
            containerFactory = "notificationKafkaListenerContainerFactory")
    public void onPaymentRefunded(ConsumerRecord<String, PaymentRefunded> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "notification-service onPaymentRefunded", record, () -> {
            notificationService.onPaymentRefunded(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(
            topics = TopicNames.SHIPPING_SCHEDULED,
            groupId = NotificationKafkaConfig.CONSUMER_GROUP,
            containerFactory = "notificationKafkaListenerContainerFactory")
    public void onShipmentCreated(ConsumerRecord<String, ShipmentCreated> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "notification-service onShipmentCreated", record, () -> {
            notificationService.onShipmentCreated(record.value());
            ack.acknowledge();
        });
    }
}
