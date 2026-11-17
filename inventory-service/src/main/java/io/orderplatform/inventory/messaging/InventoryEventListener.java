package io.orderplatform.inventory.messaging;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.inventory.config.InventoryKafkaConfig;
import io.orderplatform.inventory.domain.InventoryReservationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventListener {

    private final InventoryReservationService reservationService;
    private final Tracer tracer;

    public InventoryEventListener(InventoryReservationService reservationService, Tracer tracer) {
        this.reservationService = reservationService;
        this.tracer = tracer;
    }

    @KafkaListener(
            topics = TopicNames.PAYMENT_AUTHORIZED,
            groupId = InventoryKafkaConfig.CONSUMER_GROUP,
            containerFactory = "inventoryKafkaListenerContainerFactory")
    public void onPaymentAuthorized(ConsumerRecord<String, PaymentAuthorized> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "inventory-service onPaymentAuthorized", record, () -> {
            reservationService.handlePaymentAuthorized(record.value());
            ack.acknowledge();
        });
    }

    @KafkaListener(
            topics = TopicNames.SHIPPING_FAILED,
            groupId = InventoryKafkaConfig.CONSUMER_GROUP,
            containerFactory = "inventoryKafkaListenerContainerFactory")
    public void onShipmentFailed(ConsumerRecord<String, ShipmentFailed> record, Acknowledgment ack) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "inventory-service onShipmentFailed", record, () -> {
            reservationService.handleShipmentFailed(record.value());
            ack.acknowledge();
        });
    }
}
