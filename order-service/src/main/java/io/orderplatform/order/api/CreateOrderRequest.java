package io.orderplatform.order.api;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(
        String orderId,
        @NotBlank String customerId,
        @NotBlank @Email String customerEmail,
        @NotBlank String sku,
        @Min(1) int quantity,
        @Min(1) long totalAmountCents,
        String currency,
        @NotBlank String paymentMethodToken,
        @NotBlank String shippingPostalCode,
        String couponCode
) {
}
