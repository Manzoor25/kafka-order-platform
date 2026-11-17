package io.orderplatform.payment.messaging;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.payment.config.PaymentKafkaConfig;
import io.orderplatform.payment.domain.PaymentProcessorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener {

    private final PaymentProcessorService processorService;
    private final Tracer tracer;

    public PaymentEventListener(PaymentProcessorService processorService, Tracer tracer) {
        this.processorService = processorService;
        this.tracer = tracer;
    }

    @KafkaListener(
            topics = TopicNames.ORDER_CREATED,
            groupId = PaymentKafkaConfig.CONSUMER_GROUP,
            containerFactory = "eosKafkaListenerContainerFactory")
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "payment-service onOrderCreated (EOS)", record, () ->
                processorService.processOrderCreated(record.value()));
    }

    @KafkaListener(
            topics = TopicNames.INVENTORY_REJECTED,
            groupId = PaymentKafkaConfig.CONSUMER_GROUP,
            containerFactory = "eosKafkaListenerContainerFactory")
    public void onInventoryRejected(ConsumerRecord<String, InventoryReservationFailed> record) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "payment-service onInventoryRejected (EOS)", record, () ->
                processorService.compensateOnInventoryRejected(record.value()));
    }

    @KafkaListener(
            topics = TopicNames.INVENTORY_RELEASED,
            groupId = PaymentKafkaConfig.CONSUMER_GROUP,
            containerFactory = "eosKafkaListenerContainerFactory")
    public void onInventoryReleased(ConsumerRecord<String, InventoryReleased> record) {
        KafkaTracePropagator.executeInConsumerSpan(tracer, "payment-service onInventoryReleased (EOS)", record, () ->
                processorService.compensateOnInventoryReleased(record.value()));
    }
}
