package io.orderplatform.order.domain;

public enum OrderStatus {
    PENDING,
    PAYMENT_AUTHORIZED,
    STOCK_RESERVED,
    COMPLETED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == CANCELLED;
    }
}
