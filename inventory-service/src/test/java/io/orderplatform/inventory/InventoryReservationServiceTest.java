package io.orderplatform.inventory;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.inventory.domain.InventoryReservationService;
import io.orderplatform.inventory.domain.InventoryStockRepository;
import io.orderplatform.inventory.domain.InventoryStockRepository.ReservationDecision;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InventoryReservationServiceTest {

    private InventoryStockRepository stockRepository;
    private ProcessedEventStore processedEventStore;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private InventoryReservationService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        stockRepository = mock(InventoryStockRepository.class);
        processedEventStore = mock(ProcessedEventStore.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        SdkTracerProvider provider = SdkTracerProvider.builder().build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        service = new InventoryReservationService(
                stockRepository, processedEventStore, kafkaTemplate, otel.getTracer("inv-test"));
    }

    @Test
    void sufficientStockEmitsInventoryReservedAndMarksEventProcessed() {
        when(processedEventStore.isProcessed("evt-pay-1")).thenReturn(false);
        when(stockRepository.reserveIdempotently("ord-1", "SKU-MECH-KB", 2))
                .thenReturn(new ReservationDecision(true, "res-ord-1", "SKU-MECH-KB", 2, 48, "RESERVED", null));

        PaymentAuthorized event = new PaymentAuthorized(
                "evt-pay-1", "ord-1", "pay-1", "ch_1", "SKU-MECH-KB", 2,
                25800L, "USD", "98101", "buyer@example.com", Instant.now().toEpochMilli());

        service.handlePaymentAuthorized(event);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TopicNames.INVENTORY_RESERVED);
        InventoryReserved payload = (InventoryReserved) captor.getValue().value();
        assertThat(payload.getReservationId()).isEqualTo("res-ord-1");
        assertThat(payload.getQuantity()).isEqualTo(2);
    }

    @Test
    void outOfStockEmitsInventoryReservationFailedToTriggerPaymentRefund() {
        when(processedEventStore.isProcessed("evt-pay-oos")).thenReturn(false);
        when(stockRepository.reserveIdempotently("ord-oos", "SKU-OUT-OF-STOCK", 1))
                .thenReturn(new ReservationDecision(
                        false, "res-oos", "SKU-OUT-OF-STOCK", 1, 0, "REJECTED", "Insufficient stock"));

        PaymentAuthorized event = new PaymentAuthorized(
                "evt-pay-oos", "ord-oos", "pay-2", "ch_2", "SKU-OUT-OF-STOCK", 1,
                3900L, "USD", "98101", "oos@example.com", Instant.now().toEpochMilli());

        service.handlePaymentAuthorized(event);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TopicNames.INVENTORY_REJECTED);
        InventoryReservationFailed payload = (InventoryReservationFailed) captor.getValue().value();
        assertThat(payload.getAvailableQuantity()).isZero();
        assertThat(payload.getReason()).isEqualTo("Insufficient stock");
    }

    @Test
    void shipmentFailedReleasesReservedStockAndEmitsInventoryReleased() {
        when(processedEventStore.isProcessed("evt-ship-fail")).thenReturn(false);
        when(stockRepository.releaseIdempotently("ord-ship-fail", "Unserviceable zone"))
                .thenReturn(Optional.of(new ReservationDecision(
                        false, "res-sf", "SKU-HEADSET", 3, 50, "RELEASED", "Unserviceable zone")));

        ShipmentFailed failed = new ShipmentFailed(
                "evt-ship-fail", "ord-ship-fail", "SKU-HEADSET", 3, "FAIL-SHIP",
                "Unserviceable zone", "sf@example.com", Instant.now().toEpochMilli());

        service.handleShipmentFailed(failed);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(TopicNames.INVENTORY_RELEASED);
        InventoryReleased payload = (InventoryReleased) captor.getValue().value();
        assertThat(payload.getReleasedQuantity()).isEqualTo(3);
    }

    @Test
    void duplicatePaymentAuthorizedEventIsSkippedIdempotently() {
        when(processedEventStore.isProcessed("evt-dup")).thenReturn(true);

        service.handlePaymentAuthorized(new PaymentAuthorized(
                "evt-dup", "ord-dup", "pay-dup", "ch_dup", "SKU-MOUSE", 1,
                4900L, "USD", "98101", "dup@example.com", Instant.now().toEpochMilli()));

        verify(stockRepository, never()).reserveIdempotently(any(), any(), any(Integer.class));
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }
}
