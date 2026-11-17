package io.orderplatform.order.domain;

public record CreateOrderCommand(
        String orderId,
        String customerId,
        String customerEmail,
        String sku,
        int quantity,
        long totalAmountCents,
        String currency,
        String paymentMethodToken,
        String shippingPostalCode,
        String couponCode
) {
}
