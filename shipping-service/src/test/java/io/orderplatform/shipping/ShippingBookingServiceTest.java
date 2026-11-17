package io.orderplatform.shipping;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.shipping.domain.ShipmentRepository;
import io.orderplatform.shipping.domain.ShipmentRepository.ShipmentOutcome;
import io.orderplatform.shipping.domain.ShippingBookingService;
import io.orderplatform.shipping.domain.TransientCarrierException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShippingBookingServiceTest {

    private ShipmentRepository shipmentRepository;
    private ProcessedEventStore processedEventStore;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private ShippingBookingService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        shipmentRepository = mock(ShipmentRepository.class);
        processedEventStore = mock(ProcessedEventStore.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        SdkTracerProvider provider = SdkTracerProvider.builder().build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        service = new ShippingBookingService(
                shipmentRepository, processedEventStore, kafkaTemplate, otel.getTracer("ship-test"));
    }

    @Test
    void validPostalCodeSchedulesShipmentAndEmitsShipmentCreated() {
        when(processedEventStore.isProcessed("evt-inv-1")).thenReturn(false);
        when(shipmentRepository.bookOrRejectIdempotently(
                anyString(), anyString(), anyString(), anyInt(), anyString(), anyBoolean(), any()))
                .thenReturn(new ShipmentOutcome(true, "ship-1", "UPS", "UPS-98101-ABCD1234", null));

        InventoryReserved event = new InventoryReserved(
                "evt-inv-1", "ord-1", "res-1", "SKU-HEADSET", 1, "98101", "a@b.com", Instant.now().toEpochMilli());

        service.handleInventoryReserved(event, false);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TopicNames.SHIPPING_SCHEDULED);
        ShipmentCreated created = (ShipmentCreated) captor.getValue().value();
        assertThat(created.getTrackingNumber()).isEqualTo("UPS-98101-ABCD1234");
    }

    @Test
    void failShipPostalCodeEmitsShipmentFailedForSagaCompensationWithoutRetry() {
        when(processedEventStore.isProcessed("evt-inv-fail")).thenReturn(false);
        when(shipmentRepository.bookOrRejectIdempotently(
                anyString(), anyString(), anyString(), anyInt(), anyString(), anyBoolean(), anyString()))
                .thenReturn(new ShipmentOutcome(false, "ship-f", null, null, "Carrier cannot service postal zone: FAIL-SHIP-ZONE"));

        InventoryReserved event = new InventoryReserved(
                "evt-inv-fail", "ord-fail", "res-f", "SKU-HEADSET", 1,
                "FAIL-SHIP-ZONE", "a@b.com", Instant.now().toEpochMilli());

        service.handleInventoryReserved(event, false);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TopicNames.SHIPPING_FAILED);
        ShipmentFailed failed = (ShipmentFailed) captor.getValue().value();
        assertThat(failed.getReason()).contains("FAIL-SHIP-ZONE");
    }

    @Test
    void transientPoisonThrowsDuringNormalProcessingAndSucceedsWhenReplayedFromDlt() {
        when(processedEventStore.isProcessed("evt-poison")).thenReturn(false);
        when(shipmentRepository.bookOrRejectIdempotently(
                anyString(), anyString(), anyString(), anyInt(), anyString(), anyBoolean(), any()))
                .thenReturn(new ShipmentOutcome(true, "ship-replayed", "DHL", "DHL-REPLAY-OK", null));

        InventoryReserved poisonEvent = new InventoryReserved(
                "evt-poison", "ord-poison", "res-p", "SKU-MECH-KB", 1,
                ShippingBookingService.POSTAL_POISON_TRANSIENT, "p@b.com", Instant.now().toEpochMilli());

        assertThatThrownBy(() -> service.handleInventoryReserved(poisonEvent, false))
                .isInstanceOf(TransientCarrierException.class);

        // When replayed via dlq-cli with x-dlq-replayed-at header, carrier recovers and books shipment
        service.handleInventoryReserved(poisonEvent, true);
        verify(kafkaTemplate).send(any(ProducerRecord.class));
    }
}
