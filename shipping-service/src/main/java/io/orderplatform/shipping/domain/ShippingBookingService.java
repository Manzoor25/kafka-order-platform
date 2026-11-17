package io.orderplatform.shipping.domain;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.shipping.config.ShippingKafkaConfig;
import io.orderplatform.shipping.domain.ShipmentRepository.ShipmentOutcome;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class ShippingBookingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingBookingService.class);

    public static final String REPLAYED_HEADER = "x-dlq-replayed-at";
    public static final String POSTAL_POISON_TRANSIENT = "POISON-TRANSIENT";
    public static final String POSTAL_POISON_ALWAYS = "POISON-ALWAYS";
    public static final String POSTAL_FAIL_SHIP_PREFIX = "FAIL-SHIP";

    private final ShipmentRepository shipmentRepository;
    private final ProcessedEventStore processedEventStore;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Tracer tracer;

    public ShippingBookingService(
            ShipmentRepository shipmentRepository,
            ProcessedEventStore processedEventStore,
            KafkaTemplate<String, Object> shippingKafkaTemplate,
            Tracer tracer) {
        this.shipmentRepository = shipmentRepository;
        this.processedEventStore = processedEventStore;
        this.kafkaTemplate = shippingKafkaTemplate;
        this.tracer = tracer;
    }

    public void handleInventoryReserved(InventoryReserved event, boolean replayedFromDlt) {
        if (processedEventStore.isProcessed(event.getEventId())) {
            log.info("Skipping already processed InventoryReserved eventId={} orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        String postalCode = event.getShippingPostalCode() == null ? "" : event.getShippingPostalCode().trim();
        if (POSTAL_POISON_ALWAYS.equalsIgnoreCase(postalCode)) {
            throw new TransientCarrierException(
                    "Unparseable carrier routing code postalCode=" + postalCode + " for orderId=" + event.getOrderId());
        }
        if (POSTAL_POISON_TRANSIENT.equalsIgnoreCase(postalCode) && !replayedFromDlt) {
            throw new TransientCarrierException(
                    "Simulated carrier gateway 503 outage for orderId=" + event.getOrderId() + " (recoverable on DLT replay)");
        }

        boolean businessRejected = postalCode.toUpperCase().startsWith(POSTAL_FAIL_SHIP_PREFIX);
        String rejectionReason = businessRejected
                ? "Carrier cannot service postal zone: " + postalCode
                : null;

        ShipmentOutcome outcome = shipmentRepository.bookOrRejectIdempotently(
                event.getOrderId(),
                event.getReservationId(),
                event.getSku(),
                event.getQuantity(),
                postalCode.isEmpty() ? "00000" : postalCode,
                businessRejected,
                rejectionReason
        );

        if (outcome.scheduled()) {
            String outEventId = UUID.nameUUIDFromBytes(
                    ("ship-ok:" + event.getOrderId()).getBytes(StandardCharsets.UTF_8)).toString();
            ShipmentCreated created = ShipmentCreated.newBuilder()
                    .setEventId(outEventId)
                    .setOrderId(event.getOrderId())
                    .setShipmentId(outcome.shipmentId())
                    .setCarrier(outcome.carrier())
                    .setTrackingNumber(outcome.trackingNumber())
                    .setShippingPostalCode(postalCode)
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            sendSynchronously(TopicNames.SHIPPING_SCHEDULED, event.getOrderId(), created);
        } else {
            String outEventId = UUID.nameUUIDFromBytes(
                    ("ship-fail:" + event.getOrderId()).getBytes(StandardCharsets.UTF_8)).toString();
            ShipmentFailed failed = ShipmentFailed.newBuilder()
                    .setEventId(outEventId)
                    .setOrderId(event.getOrderId())
                    .setSku(event.getSku())
                    .setQuantity(event.getQuantity())
                    .setShippingPostalCode(postalCode)
                    .setReason(outcome.failureReason())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            sendSynchronously(TopicNames.SHIPPING_FAILED, event.getOrderId(), failed);
        }

        processedEventStore.tryClaim(
                event.getEventId(), "InventoryReserved", event.getOrderId(), ShippingKafkaConfig.CONSUMER_GROUP);
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
}
