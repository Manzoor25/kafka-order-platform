package io.orderplatform.order.domain;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.InventoryReserved;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.events.avro.ShipmentFailed;
import io.orderplatform.order.config.OrderKafkaConfig;
import io.orderplatform.order.outbox.AvroJsonCodec;
import io.orderplatform.order.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Coordinates order lifecycle state transitions and writes outbound domain events atomically
 * to {@code outbox_events} within the same PostgreSQL ACID transaction as {@code orders}.
 */
@Service
public class OrderSagaService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ProcessedEventStore processedEventStore;
    private final Tracer tracer;

    public OrderSagaService(
            OrderRepository orderRepository,
            OutboxRepository outboxRepository,
            ProcessedEventStore processedEventStore,
            Tracer tracer) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.processedEventStore = processedEventStore;
        this.tracer = tracer;
    }

    @Transactional
    public OrderRecord createOrder(CreateOrderCommand cmd) {
        String orderId = (cmd.orderId() == null || cmd.orderId().isBlank())
                ? "ord-" + UUID.randomUUID()
                : cmd.orderId();

        Optional<OrderRecord> existing = orderRepository.findById(orderId);
        if (existing.isPresent()) {
            return existing.get();
        }

        String traceId = KafkaTracePropagator.currentTraceId();
        String traceparent = KafkaTracePropagator.currentTraceparent();
        if (traceId == null || traceparent == null) {
            Span rootSpan = tracer.spanBuilder("orders.create").startSpan();
            try (Scope ignored = rootSpan.makeCurrent()) {
                traceId = KafkaTracePropagator.currentTraceId();
                traceparent = KafkaTracePropagator.currentTraceparent();
            } finally {
                rootSpan.end();
            }
        }

        String currency = (cmd.currency() == null || cmd.currency().isBlank()) ? "USD" : cmd.currency();
        Instant now = Instant.now();

        OrderRecord record = new OrderRecord(
                orderId,
                cmd.customerId(),
                cmd.customerEmail(),
                cmd.sku(),
                cmd.quantity(),
                cmd.totalAmountCents(),
                currency,
                cmd.paymentMethodToken(),
                cmd.shippingPostalCode(),
                cmd.couponCode(),
                OrderStatus.PENDING,
                null,
                null,
                null,
                null,
                null,
                null,
                traceId,
                now,
                now
        );
        orderRepository.insertOrder(record);
        orderRepository.appendHistory(orderId, null, OrderStatus.PENDING.name(), "OrderCreated",
                "Order accepted and staged in transactional outbox");

        UUID eventUuid = UUID.randomUUID();
        OrderCreated event = OrderCreated.newBuilder()
                .setEventId(eventUuid.toString())
                .setOrderId(orderId)
                .setCustomerId(cmd.customerId())
                .setCustomerEmail(cmd.customerEmail())
                .setSku(cmd.sku())
                .setQuantity(cmd.quantity())
                .setTotalAmountCents(cmd.totalAmountCents())
                .setCurrency(currency)
                .setPaymentMethodToken(cmd.paymentMethodToken())
                .setShippingPostalCode(cmd.shippingPostalCode())
                .setOccurredAtEpochMs(now.toEpochMilli())
                .setCouponCode(cmd.couponCode())
                .build();

        outboxRepository.insert(
                eventUuid,
                "ORDER",
                orderId,
                TopicNames.ORDER_CREATED,
                "OrderCreated",
                AvroJsonCodec.toJson(event),
                traceparent
        );

        return orderRepository.findById(orderId).orElseThrow();
    }

    @Transactional
    public void onPaymentAuthorized(PaymentAuthorized event) {
        if (!claim(event.getEventId(), "PaymentAuthorized", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order -> {
            if (!order.status().isTerminal() && order.status() == OrderStatus.PENDING) {
                orderRepository.markPaymentAuthorized(order.orderId(), event.getPaymentId());
            }
            orderRepository.appendHistory(order.orderId(), order.status().name(),
                    OrderStatus.PAYMENT_AUTHORIZED.name(), "PaymentAuthorized",
                    "Payment authorized: " + event.getGatewayChargeId());
        });
    }

    @Transactional
    public void onPaymentDeclined(PaymentDeclined event) {
        if (!claim(event.getEventId(), "PaymentDeclined", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order -> {
            if (order.status().isTerminal()) {
                return;
            }
            orderRepository.markCancelled(order.orderId(), "PAYMENT", event.getReason());
            orderRepository.appendHistory(order.orderId(), order.status().name(),
                    OrderStatus.CANCELLED.name(), "PaymentDeclined", event.getReason());
            stageOrderCancelledOutbox(order, "PAYMENT", event.getReason());
        });
    }

    @Transactional
    public void onInventoryReserved(InventoryReserved event) {
        if (!claim(event.getEventId(), "InventoryReserved", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order -> {
            if (!order.status().isTerminal()) {
                orderRepository.markStockReserved(order.orderId(), event.getReservationId());
            }
            orderRepository.appendHistory(order.orderId(), order.status().name(),
                    OrderStatus.STOCK_RESERVED.name(), "InventoryReserved",
                    "Reserved " + event.getQuantity() + "x " + event.getSku());
        });
    }

    @Transactional
    public void onInventoryReservationFailed(InventoryReservationFailed event) {
        if (!claim(event.getEventId(), "InventoryReservationFailed", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order ->
                orderRepository.appendHistory(order.orderId(), order.status().name(),
                        "COMPENSATING_PAYMENT_REFUND", "InventoryReservationFailed", event.getReason()));
    }

    @Transactional
    public void onShipmentFailed(ShipmentFailed event) {
        if (!claim(event.getEventId(), "ShipmentFailed", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order ->
                orderRepository.appendHistory(order.orderId(), order.status().name(),
                        "COMPENSATING_INVENTORY_RELEASE", "ShipmentFailed", event.getReason()));
    }

    @Transactional
    public void onInventoryReleased(InventoryReleased event) {
        if (!claim(event.getEventId(), "InventoryReleased", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order ->
                orderRepository.appendHistory(order.orderId(), order.status().name(),
                        "INVENTORY_RELEASED_AWAITING_REFUND", "InventoryReleased", event.getReason()));
    }

    @Transactional
    public void onPaymentRefunded(PaymentRefunded event) {
        if (!claim(event.getEventId(), "PaymentRefunded", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order -> {
            if (order.status().isTerminal()) {
                return;
            }
            String stage = event.getCompensationTrigger();
            orderRepository.markCancelled(order.orderId(), stage, event.getReason());
            orderRepository.appendHistory(order.orderId(), order.status().name(),
                    OrderStatus.CANCELLED.name(), "PaymentRefunded",
                    "Refunded (" + event.getGatewayRefundId() + "): " + event.getReason());
            stageOrderCancelledOutbox(order, stage, event.getReason());
        });
    }

    @Transactional
    public void onShipmentCreated(ShipmentCreated event) {
        if (!claim(event.getEventId(), "ShipmentCreated", event.getOrderId())) {
            return;
        }
        orderRepository.findByIdForUpdate(event.getOrderId()).ifPresent(order -> {
            if (order.status().isTerminal()) {
                return;
            }
            orderRepository.markCompleted(order.orderId(), event.getShipmentId(), event.getTrackingNumber());
            orderRepository.appendHistory(order.orderId(), order.status().name(),
                    OrderStatus.COMPLETED.name(), "ShipmentCreated",
                    "Tracking " + event.getTrackingNumber() + " via " + event.getCarrier());

            UUID eventUuid = UUID.randomUUID();
            OrderConfirmed confirmed = OrderConfirmed.newBuilder()
                    .setEventId(eventUuid.toString())
                    .setOrderId(order.orderId())
                    .setCustomerId(order.customerId())
                    .setCustomerEmail(order.customerEmail())
                    .setShipmentId(event.getShipmentId())
                    .setTrackingNumber(event.getTrackingNumber())
                    .setTotalAmountCents(order.totalAmountCents())
                    .setCurrency(order.currency())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();

            outboxRepository.insert(
                    eventUuid,
                    "ORDER",
                    order.orderId(),
                    TopicNames.ORDER_CONFIRMED,
                    "OrderConfirmed",
                    AvroJsonCodec.toJson(confirmed),
                    KafkaTracePropagator.currentTraceparent()
            );
        });
    }

    @Transactional(readOnly = true)
    public Optional<OrderRecord> findOrder(String orderId) {
        return orderRepository.findById(orderId);
    }

    @Transactional(readOnly = true)
    public List<SagaHistoryEntry> findHistory(String orderId) {
        return orderRepository.findHistory(orderId);
    }

    @Transactional(readOnly = true)
    public List<OrderRecord> findRecentOrders(int limit) {
        return orderRepository.findRecent(limit);
    }

    private boolean claim(String eventId, String eventType, String orderId) {
        return processedEventStore.tryClaim(eventId, eventType, orderId, OrderKafkaConfig.CONSUMER_GROUP);
    }

    private void stageOrderCancelledOutbox(OrderRecord order, String failedStage, String reason) {
        UUID eventUuid = UUID.randomUUID();
        OrderCancelled cancelled = OrderCancelled.newBuilder()
                .setEventId(eventUuid.toString())
                .setOrderId(order.orderId())
                .setCustomerId(order.customerId())
                .setCustomerEmail(order.customerEmail())
                .setFailedStage(failedStage)
                .setReason(reason)
                .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                .build();

        outboxRepository.insert(
                eventUuid,
                "ORDER",
                order.orderId(),
                TopicNames.ORDER_CANCELLED,
                "OrderCancelled",
                AvroJsonCodec.toJson(cancelled),
                KafkaTracePropagator.currentTraceparent()
        );
    }
}
