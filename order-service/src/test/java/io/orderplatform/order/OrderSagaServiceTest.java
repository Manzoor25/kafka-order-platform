package io.orderplatform.order;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.order.domain.CreateOrderCommand;
import io.orderplatform.order.domain.OrderRecord;
import io.orderplatform.order.domain.OrderRepository;
import io.orderplatform.order.domain.OrderSagaService;
import io.orderplatform.order.domain.OrderStatus;
import io.orderplatform.order.outbox.AvroJsonCodec;
import io.orderplatform.order.outbox.OutboxEventRecord;
import io.orderplatform.order.outbox.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderSagaServiceTest {

    private OrderRepository orderRepository;
    private OutboxRepository outboxRepository;
    private ProcessedEventStore processedEventStore;
    private OrderSagaService sagaService;
    private AtomicReference<OrderRecord> storedOrder;

    @BeforeEach
    void setUp() {
        orderRepository = mock(OrderRepository.class);
        outboxRepository = mock(OutboxRepository.class);
        processedEventStore = mock(ProcessedEventStore.class);
        storedOrder = new AtomicReference<>();

        SdkTracerProvider provider = SdkTracerProvider.builder().build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        sagaService = new OrderSagaService(
                orderRepository, outboxRepository, processedEventStore, otel.getTracer("order-test"));

        doAnswer(inv -> {
            storedOrder.set(inv.getArgument(0));
            return null;
        }).when(orderRepository).insertOrder(any());
        when(orderRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(storedOrder.get()));
        when(orderRepository.findByIdForUpdate(anyString())).thenAnswer(inv -> Optional.ofNullable(storedOrder.get()));
    }

    @Test
    void createOrderStagesOrderCreatedInTransactionalOutboxWithTraceparent() {
        when(orderRepository.findById("ord-unit-1"))
                .thenReturn(Optional.empty())
                .thenAnswer(inv -> Optional.ofNullable(storedOrder.get()));

        OrderRecord created = sagaService.createOrder(new CreateOrderCommand(
                "ord-unit-1", "cust-1", "alice@example.com", "SKU-MECH-KB",
                1, 12900L, "USD", "tok_visa_ok", "98101", "SPRING10"
        ));

        assertThat(created.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(created.traceId()).hasSize(32);

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> traceCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxRepository).insert(
                any(UUID.class), eq("ORDER"), eq("ord-unit-1"),
                eq(TopicNames.ORDER_CREATED), eq("OrderCreated"),
                jsonCaptor.capture(), traceCaptor.capture());

        OrderCreated decoded = (OrderCreated) AvroJsonCodec.fromJson("OrderCreated", jsonCaptor.getValue());
        assertThat(decoded.getOrderId()).isEqualTo("ord-unit-1");
        assertThat(decoded.getCouponCode()).isEqualTo("SPRING10");
        assertThat(traceCaptor.getValue()).startsWith("00-" + created.traceId() + "-");
    }

    @Test
    void happyPathTransitionsOrderToCompletedAndStagesOrderConfirmed() {
        storedOrder.set(sampleOrder("ord-happy", OrderStatus.PENDING));
        when(processedEventStore.tryClaim(anyString(), anyString(), eq("ord-happy"), anyString())).thenReturn(true);

        sagaService.onPaymentAuthorized(new PaymentAuthorized(
                "evt-1", "ord-happy", "pay-1", "ch_1", "SKU-KB", 1, 9900L, "USD", "98101", "a@b.com", 1000L));
        verify(orderRepository).markPaymentAuthorized("ord-happy", "pay-1");

        storedOrder.set(sampleOrder("ord-happy", OrderStatus.PAYMENT_AUTHORIZED));
        sagaService.onInventoryReserved(new InventoryReserved(
                "evt-2", "ord-happy", "res-1", "SKU-KB", 1, "98101", "a@b.com", 2000L));
        verify(orderRepository).markStockReserved("ord-happy", "res-1");

        storedOrder.set(sampleOrder("ord-happy", OrderStatus.STOCK_RESERVED));
        sagaService.onShipmentCreated(new ShipmentCreated(
                "evt-3", "ord-happy", "ship-1", "UPS", "1Z999AA101", "98101", "a@b.com", 3000L));
        verify(orderRepository).markCompleted("ord-happy", "ship-1", "1Z999AA101");

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxRepository).insert(any(), eq("ORDER"), eq("ord-happy"),
                eq(TopicNames.ORDER_CONFIRMED), eq("OrderConfirmed"), jsonCaptor.capture(), any());
        OrderConfirmed confirmed = (OrderConfirmed) AvroJsonCodec.fromJson("OrderConfirmed", jsonCaptor.getValue());
        assertThat(confirmed.getTrackingNumber()).isEqualTo("1Z999AA101");
    }

    @Test
    void shippingFailureWaitsForPaymentRefundedBeforeMarkingOrderCancelled() {
        storedOrder.set(sampleOrder("ord-comp", OrderStatus.STOCK_RESERVED));
        when(processedEventStore.tryClaim(anyString(), anyString(), eq("ord-comp"), anyString())).thenReturn(true);

        sagaService.onShipmentFailed(new ShipmentFailed(
                "evt-sf", "ord-comp", "SKU-KB", 1, "FAIL-SHIP", "Unserviceable zone", "a@b.com", 4000L));
        verify(orderRepository, never()).markCancelled(anyString(), anyString(), anyString());

        sagaService.onPaymentRefunded(new PaymentRefunded(
                "evt-pr", "ord-comp", "pay-1", "re_1", 9900L, "USD",
                "SHIPPING_FAILED", "Unserviceable zone", "a@b.com", 5000L));
        verify(orderRepository).markCancelled("ord-comp", "SHIPPING_FAILED", "Unserviceable zone");

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxRepository).insert(any(), eq("ORDER"), eq("ord-comp"),
                eq(TopicNames.ORDER_CANCELLED), eq("OrderCancelled"), jsonCaptor.capture(), any());
        OrderCancelled cancelled = (OrderCancelled) AvroJsonCodec.fromJson("OrderCancelled", jsonCaptor.getValue());
        assertThat(cancelled.getFailedStage()).isEqualTo("SHIPPING_FAILED");
    }

    @Test
    void duplicatePaymentDeclinedEventIsIgnoredByProcessedEventStore() {
        storedOrder.set(sampleOrder("ord-dup", OrderStatus.PENDING));
        when(processedEventStore.tryClaim(eq("evt-dup"), eq("PaymentDeclined"), eq("ord-dup"), anyString()))
                .thenReturn(false);

        sagaService.onPaymentDeclined(new PaymentDeclined(
                "evt-dup", "ord-dup", "pay-x", 9900L, "USD", "Card declined", "a@b.com", 1000L));

        verify(orderRepository, never()).markCancelled(anyString(), anyString(), anyString());
        verify(outboxRepository, never()).insert(any(), any(), any(), any(), any(), any(), any());
    }

    private static OrderRecord sampleOrder(String orderId, OrderStatus status) {
        Instant now = Instant.now();
        return new OrderRecord(
                orderId, "cust-1", "a@b.com", "SKU-KB", 1, 9900L, "USD",
                "tok_visa_ok", "98101", null, status,
                null, null, null, null, null, null, "0123456789abcdef0123456789abcdef", now, now
        );
    }
}
