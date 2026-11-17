package io.orderplatform.inventory.domain;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.inventory.config.InventoryKafkaConfig;
import io.orderplatform.inventory.domain.InventoryStockRepository.ReservationDecision;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class InventoryReservationService {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationService.class);

    private final InventoryStockRepository stockRepository;
    private final ProcessedEventStore processedEventStore;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Tracer tracer;

    public InventoryReservationService(
            InventoryStockRepository stockRepository,
            ProcessedEventStore processedEventStore,
            KafkaTemplate<String, Object> inventoryKafkaTemplate,
            Tracer tracer) {
        this.stockRepository = stockRepository;
        this.processedEventStore = processedEventStore;
        this.kafkaTemplate = inventoryKafkaTemplate;
        this.tracer = tracer;
    }

    public void handlePaymentAuthorized(PaymentAuthorized event) {
        if (processedEventStore.isProcessed(event.getEventId())) {
            log.info("Skipping duplicate PaymentAuthorized eventId={} orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        ReservationDecision decision = stockRepository.reserveIdempotently(
                event.getOrderId(), event.getSku(), event.getQuantity());

        if (decision.reserved()) {
            String outEventId = deterministicEventId("inv-reserved:" + event.getOrderId());
            InventoryReserved reserved = InventoryReserved.newBuilder()
                    .setEventId(outEventId)
                    .setOrderId(event.getOrderId())
                    .setReservationId(decision.reservationId())
                    .setSku(event.getSku())
                    .setQuantity(event.getQuantity())
                    .setShippingPostalCode(event.getShippingPostalCode())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            sendSynchronously(TopicNames.INVENTORY_RESERVED, event.getOrderId(), reserved);
        } else {
            String outEventId = deterministicEventId("inv-rejected:" + event.getOrderId());
            InventoryReservationFailed failed = InventoryReservationFailed.newBuilder()
                    .setEventId(outEventId)
                    .setOrderId(event.getOrderId())
                    .setSku(event.getSku())
                    .setRequestedQuantity(event.getQuantity())
                    .setAvailableQuantity(decision.availableQuantity())
                    .setReason(decision.reason())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            sendSynchronously(TopicNames.INVENTORY_REJECTED, event.getOrderId(), failed);
        }

        processedEventStore.tryClaim(
                event.getEventId(), "PaymentAuthorized", event.getOrderId(), InventoryKafkaConfig.CONSUMER_GROUP);
    }

    public void handleShipmentFailed(ShipmentFailed event) {
        if (processedEventStore.isProcessed(event.getEventId())) {
            log.info("Skipping duplicate ShipmentFailed eventId={} orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        Optional<ReservationDecision> releasedOpt =
                stockRepository.releaseIdempotently(event.getOrderId(), event.getReason());

        if (releasedOpt.isPresent()) {
            ReservationDecision released = releasedOpt.get();
            String outEventId = deterministicEventId("inv-released:" + event.getOrderId());
            InventoryReleased out = InventoryReleased.newBuilder()
                    .setEventId(outEventId)
                    .setOrderId(event.getOrderId())
                    .setReservationId(released.reservationId())
                    .setSku(released.sku())
                    .setReleasedQuantity(released.quantity())
                    .setReason(event.getReason())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            sendSynchronously(TopicNames.INVENTORY_RELEASED, event.getOrderId(), out);
        }

        processedEventStore.tryClaim(
                event.getEventId(), "ShipmentFailed", event.getOrderId(), InventoryKafkaConfig.CONSUMER_GROUP);
    }

    private void sendSynchronously(String topic, String key, Object payload) {
        ProducerRecord<String, Object> outRecord =
                KafkaTracePropagator.tracedProducerRecord(tracer, topic, key, payload);
        try {
            kafkaTemplate.send(outRecord).get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed awaiting broker ack for " + topic + " key=" + key, ex);
        }
    }

    private static String deterministicEventId(String seed) {
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
