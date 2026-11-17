package io.orderplatform.events;

import java.util.List;

/**
 * Canonical Kafka topic catalog for the choreographed order platform.
 * Every topic is keyed by {@code orderId} to guarantee per-order causal partition ordering.
 */
public final class TopicNames {

    public static final String ORDER_CREATED = "order.created.v1";
    public static final String ORDER_CONFIRMED = "order.confirmed.v1";
    public static final String ORDER_CANCELLED = "order.cancelled.v1";

    public static final String PAYMENT_AUTHORIZED = "payment.authorized.v1";
    public static final String PAYMENT_DECLINED = "payment.declined.v1";
    public static final String PAYMENT_REFUNDED = "payment.refunded.v1";

    public static final String INVENTORY_RESERVED = "inventory.reserved.v1";
    public static final String INVENTORY_REJECTED = "inventory.rejected.v1";
    public static final String INVENTORY_RELEASED = "inventory.released.v1";

    public static final String SHIPPING_SCHEDULED = "shipping.scheduled.v1";
    public static final String SHIPPING_FAILED = "shipping.failed.v1";

    public static final List<String> ALL_DOMAIN_TOPICS = List.of(
            ORDER_CREATED,
            ORDER_CONFIRMED,
            ORDER_CANCELLED,
            PAYMENT_AUTHORIZED,
            PAYMENT_DECLINED,
            PAYMENT_REFUNDED,
            INVENTORY_RESERVED,
            INVENTORY_REJECTED,
            INVENTORY_RELEASED,
            SHIPPING_SCHEDULED,
            SHIPPING_FAILED
    );

    private TopicNames() {
    }

    public static String retryTopic(String baseTopic, int attemptIndex) {
        return baseTopic + ".retry-" + attemptIndex;
    }

    public static String dltTopic(String baseTopic) {
        return baseTopic + ".dlt";
    }
}
