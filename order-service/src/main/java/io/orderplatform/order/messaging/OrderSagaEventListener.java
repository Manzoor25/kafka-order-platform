package io.orderplatform.order.messaging;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.order.config.OrderKafkaConfig;
import io.orderplatform.order.domain.OrderSagaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderSagaEventListener {

    private final OrderSagaService sagaService;
    private final Tracer tracer;

    public OrderSagaEventListener(OrderSagaService sagaService, Tracer tracer) {
        this.sagaService = sagaService;
        this.tracer = tracer;
    }

    @KafkaListener(topics = TopicNames.PAYMENT_AUTHORIZED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handlePaymentAuthorized(ConsumerRecord<String, PaymentAuthorized> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onPaymentAuthorized", record, () -> {
            sagaService.onPaymentAuthorized(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.PAYMENT_DECLINED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handlePaymentDeclined(ConsumerRecord<String, PaymentDeclined> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onPaymentDeclined", record, () -> {
            sagaService.onPaymentDeclined(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.INVENTORY_RESERVED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handleInventoryReserved(ConsumerRecord<String, InventoryReserved> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onInventoryReserved", record, () -> {
            sagaService.onInventoryReserved(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.INVENTORY_REJECTED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handleInventoryRejected(ConsumerRecord<String, InventoryReservationFailed> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onInventoryRejected", record, () -> {
            sagaService.onInventoryReservationFailed(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.SHIPPING_FAILED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handleShipmentFailed(ConsumerRecord<String, ShipmentFailed> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onShipmentFailed", record, () -> {
            sagaService.onShipmentFailed(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.INVENTORY_RELEASED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handleInventoryReleased(ConsumerRecord<String, InventoryReleased> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onInventoryReleased", record, () -> {
            sagaService.onInventoryReleased(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.PAYMENT_REFUNDED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handlePaymentRefunded(ConsumerRecord<String, PaymentRefunded> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onPaymentRefunded", record, () -> {
            sagaService.onPaymentRefunded(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(topics = TopicNames.SHIPPING_SCHEDULED, groupId = OrderKafkaConfig.CONSUMER_GROUP)
    public void handleShipmentCreated(ConsumerRecord<String, ShipmentCreated> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "order-service onShipmentCreated", record, () -> {
            sagaService.onShipmentCreated(record.value());
            ack.acknowledge();
        });
    }
}
