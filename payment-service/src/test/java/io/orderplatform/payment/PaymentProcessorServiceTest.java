package io.orderplatform.payment;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.payment.domain.PaymentLedgerRepository;
import io.orderplatform.payment.domain.PaymentLedgerRepository.PaymentRecord;
import io.orderplatform.payment.domain.PaymentProcessorService;
import io.orderplatform.payment.gateway.SimulatedCardGateway;
import io.orderplatform.payment.gateway.SimulatedCardGateway.GatewayResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentProcessorServiceTest {

    private SimulatedCardGateway cardGateway;
    private PaymentLedgerRepository ledgerRepository;
    private ProcessedEventStore processedEventStore;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private PaymentProcessorService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        cardGateway = mock(SimulatedCardGateway.class);
        ledgerRepository = mock(PaymentLedgerRepository.class);
        processedEventStore = mock(ProcessedEventStore.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
        when(txManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());

        SdkTracerProvider provider = SdkTracerProvider.builder().build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();

        service = new PaymentProcessorService(
                cardGateway, ledgerRepository, processedEventStore,
                kafkaTemplate, txManager, otel.getTracer("payment-test"), 0L);
    }

    @Test
    void approvedCardEmitsPaymentAuthorizedAndRecordsLedgerWithDeterministicIdempotencyKey() {
        when(processedEventStore.isProcessed("evt-1")).thenReturn(false);
        when(processedEventStore.tryClaim(eq("evt-1"), eq("OrderCreated"), eq("ord-1"), anyString())).thenReturn(true);
        when(ledgerRepository.findByOrderId("ord-1")).thenReturn(Optional.empty());
        when(cardGateway.authorize("charge:ord-1", "ord-1", 15900L, "USD", "tok_visa_ok"))
                .thenReturn(new GatewayResult(true, "ch_abc123", null, false));

        OrderCreated created = OrderCreated.newBuilder()
                .setEventId("evt-1")
                .setOrderId("ord-1")
                .setCustomerId("cust-1")
                .setCustomerEmail("buyer@example.com")
                .setSku("SKU-HEADSET")
                .setQuantity(1)
                .setTotalAmountCents(15900L)
                .setCurrency("USD")
                .setPaymentMethodToken("tok_visa_ok")
                .setShippingPostalCode("98101")
                .setCouponCode(null)
                .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                .build();

        service.processOrderCreated(created);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertThat(recordCaptor.getValue().topic()).isEqualTo(TopicNames.PAYMENT_AUTHORIZED);
        assertThat(recordCaptor.getValue().key()).isEqualTo("ord-1");
        PaymentAuthorized payload = (PaymentAuthorized) recordCaptor.getValue().value();
        assertThat(payload.getGatewayChargeId()).isEqualTo("ch_abc123");
        verify(ledgerRepository).upsertAuthorized(anyString(), eq("ord-1"), eq("buyer@example.com"),
                eq(15900L), eq("USD"), eq("ch_abc123"));
    }

    @Test
    void declinedCardEmitsPaymentDeclinedAndMarksLedgerDeclined() {
        when(processedEventStore.isProcessed("evt-dec")).thenReturn(false);
        when(processedEventStore.tryClaim(eq("evt-dec"), eq("OrderCreated"), eq("ord-dec"), anyString())).thenReturn(true);
        when(ledgerRepository.findByOrderId("ord-dec")).thenReturn(Optional.empty());
        when(cardGateway.authorize("charge:ord-dec", "ord-dec", 4900L, "USD", "tok_decline_insufficient"))
                .thenReturn(new GatewayResult(false, "dec_999", "Insufficient funds", false));

        OrderCreated created = OrderCreated.newBuilder()
                .setEventId("evt-dec")
                .setOrderId("ord-dec")
                .setCustomerId("cust-2")
                .setCustomerEmail("decline@example.com")
                .setSku("SKU-MOUSE")
                .setQuantity(1)
                .setTotalAmountCents(4900L)
                .setCurrency("USD")
                .setPaymentMethodToken("tok_decline_insufficient")
                .setShippingPostalCode("98101")
                .setCouponCode(null)
                .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                .build();

        service.processOrderCreated(created);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertThat(recordCaptor.getValue().topic()).isEqualTo(TopicNames.PAYMENT_DECLINED);
        PaymentDeclined declined = (PaymentDeclined) recordCaptor.getValue().value();
        assertThat(declined.getReason()).isEqualTo("Insufficient funds");
    }

    @Test
    void midTransactionCrashAbortsFirstAttemptAndDeduplicatesCardChargeOnRedelivery() {
        when(processedEventStore.isProcessed("evt-crash")).thenReturn(false);
        when(processedEventStore.tryClaim(eq("evt-crash"), eq("OrderCreated"), eq("ord-crash"), anyString())).thenReturn(true);
        when(ledgerRepository.findByOrderId("ord-crash")).thenReturn(Optional.empty());
        when(cardGateway.authorize(eq("charge:ord-crash"), eq("ord-crash"), anyLong(), anyString(), anyString()))
                .thenReturn(new GatewayResult(true, "ch_first_only", null, false))
                .thenReturn(new GatewayResult(true, "ch_first_only", null, true));

        OrderCreated created = OrderCreated.newBuilder()
                .setEventId("evt-crash")
                .setOrderId("ord-crash")
                .setCustomerId("cust-3")
                .setCustomerEmail("eos@example.com")
                .setSku("SKU-KB")
                .setQuantity(1)
                .setTotalAmountCents(12900L)
                .setCurrency("USD")
                .setPaymentMethodToken(PaymentProcessorService.TOKEN_ABORT_MID_TX_ONCE)
                .setShippingPostalCode("98101")
                .setCouponCode(null)
                .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                .build();

        // Attempt 1 throws mid-Kafka-transaction AFTER charging card & flushing uncommitted record
        assertThatThrownBy(() -> service.processOrderCreated(created))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mid-Kafka-transaction");

        // Attempt 2 (Kafka redelivery after transaction abort) reuses idempotency key "charge:ord-crash"
        service.processOrderCreated(created);

        verify(cardGateway, times(2)).authorize(
                "charge:ord-crash", "ord-crash", 12900L, "USD", PaymentProcessorService.TOKEN_ABORT_MID_TX_ONCE);
        verify(ledgerRepository, times(1)).upsertAuthorized(
                anyString(), eq("ord-crash"), eq("eos@example.com"), eq(12900L), eq("USD"), eq("ch_first_only"));
    }

    @Test
    void inventoryReleasedTriggersIdempotentRefundCompensation() {
        when(processedEventStore.isProcessed("evt-rel")).thenReturn(false);
        when(processedEventStore.tryClaim(eq("evt-rel"), eq("InventoryReleased"), eq("ord-ref"), anyString())).thenReturn(true);
        when(ledgerRepository.findByOrderId("ord-ref")).thenReturn(Optional.of(
                new PaymentRecord("pay-ref", "ord-ref", "ref@example.com", 8800L, "USD",
                        "AUTHORIZED", "ch_orig", null, null)));
        when(cardGateway.refund("refund:ord-ref", "ord-ref", 8800L, "USD", "Carrier rejected destination"))
                .thenReturn(new GatewayResult(true, "re_777", "Carrier rejected destination", false));

        InventoryReleased released = new InventoryReleased(
                "evt-rel", "ord-ref", "res-1", "SKU-KB", 1, "Carrier rejected destination",
                "ref@example.com", Instant.now().toEpochMilli());

        service.compensateOnInventoryReleased(released);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertThat(recordCaptor.getValue().topic()).isEqualTo(TopicNames.PAYMENT_REFUNDED);
        PaymentRefunded refunded = (PaymentRefunded) recordCaptor.getValue().value();
        assertThat(refunded.getGatewayRefundId()).isEqualTo("re_777");
        assertThat(refunded.getCompensationTrigger()).isEqualTo("SHIPPING_FAILED");
    }
}
