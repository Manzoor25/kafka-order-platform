package io.orderplatform.order.api;

import io.orderplatform.order.domain.OrderRecord;
import io.orderplatform.order.domain.SagaHistoryEntry;

import java.time.Instant;
import java.util.List;

public record OrderDetailResponse(
        String orderId,
        String customerId,
        String customerEmail,
        String sku,
        int quantity,
        long totalAmountCents,
        String currency,
        String shippingPostalCode,
        String couponCode,
        String status,
        String paymentId,
        String reservationId,
        String shipmentId,
        String trackingNumber,
        String failureStage,
        String failureReason,
        String traceId,
        Instant createdAt,
        Instant updatedAt,
        List<SagaHistoryEntry> history
) {
    public static OrderDetailResponse from(OrderRecord o, List<SagaHistoryEntry> history) {
        return new OrderDetailResponse(
                o.orderId(),
                o.customerId(),
                o.customerEmail(),
                o.sku(),
                o.quantity(),
                o.totalAmountCents(),
                o.currency(),
                o.shippingPostalCode(),
                o.couponCode(),
                o.status().name(),
                o.paymentId(),
                o.reservationId(),
                o.shipmentId(),
                o.trackingNumber(),
                o.failureStage(),
                o.failureReason(),
                o.traceId(),
                o.createdAt(),
                o.updatedAt(),
                history
        );
    }
}
