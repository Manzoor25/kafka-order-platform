package io.orderplatform.shipping.messaging;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.shipping.config.ShippingKafkaConfig;
import io.orderplatform.shipping.domain.ShipmentRepository;
import io.orderplatform.shipping.domain.ShippingBookingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component("shippingEventListener")
public class ShippingEventListener {

    private static final Logger log = LoggerFactory.getLogger(ShippingEventListener.class);

    private final ShippingBookingService bookingService;
    private final ShipmentRepository shipmentRepository;
    private final Tracer tracer;

    public ShippingEventListener(
            ShippingBookingService bookingService,
            ShipmentRepository shipmentRepository,
            Tracer tracer) {
        this.bookingService = bookingService;
        this.shipmentRepository = shipmentRepository;
        this.tracer = tracer;
    }

    @KafkaListener(
            topics = TopicNames.INVENTORY_RESERVED,
            groupId = ShippingKafkaConfig.CONSUMER_GROUP,
            containerFactory = "shippingKafkaListenerContainerFactory")
    public void onInventoryReserved(ConsumerRecord<String, InventoryReserved> record, Acknowledgment ack) {
        boolean replayedFromDlt = record.headers().lastHeader(ShippingBookingService.REPLAYED_HEADER) != null;
        KafkaTracePropagator.executeInConsumerSpan(tracer, "shipping-service onInventoryReserved", record, () -> {
            bookingService.handleInventoryReserved(record.value(), replayedFromDlt);
            ack.acknowledge();
        });
    }

    public void onDeadLetter(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Header exHeader = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String exMsg = exHeader == null ? "unknown" : new String(exHeader.value(), StandardCharsets.UTF_8);
        String payloadType = record.value() == null ? "RAW_CORRUPT_BYTES" : record.value().getClass().getSimpleName();
        log.error("DLT_RECORD_LANDED topic={} partition={} offset={} key={} payloadType={} reason={}",
                record.topic(), record.partition(), record.offset(), record.key(), payloadType, exMsg);
        shipmentRepository.recordDeadLetterObservation(
                record.topic(), record.partition(), record.offset(), record.key(), payloadType, exMsg);
        ack.acknowledge();
    }
}
