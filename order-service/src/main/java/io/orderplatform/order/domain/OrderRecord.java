package io.orderplatform.order.domain;

import java.time.Instant;

public record OrderRecord(
        String orderId,
        String customerId,
        String customerEmail,
        String sku,
        int quantity,
        long totalAmountCents,
        String currency,
        String paymentMethodToken,
        String shippingPostalCode,
        String couponCode,
        OrderStatus status,
        String paymentId,
        String reservationId,
        String shipmentId,
        String trackingNumber,
        String failureStage,
        String failureReason,
        String traceId,
        Instant createdAt,
        Instant updatedAt
) {
}
