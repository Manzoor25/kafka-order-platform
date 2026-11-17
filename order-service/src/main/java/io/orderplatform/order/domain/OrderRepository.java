package io.orderplatform.order.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class OrderRepository {

    private static final RowMapper<OrderRecord> ORDER_MAPPER = (rs, rowNum) -> new OrderRecord(
            rs.getString("order_id"),
            rs.getString("customer_id"),
            rs.getString("customer_email"),
            rs.getString("sku"),
            rs.getInt("quantity"),
            rs.getLong("total_amount_cents"),
            rs.getString("currency"),
            rs.getString("payment_method_token"),
            rs.getString("shipping_postal_code"),
            rs.getString("coupon_code"),
            OrderStatus.valueOf(rs.getString("status")),
            rs.getString("payment_id"),
            rs.getString("reservation_id"),
            rs.getString("shipment_id"),
            rs.getString("tracking_number"),
            rs.getString("failure_stage"),
            rs.getString("failure_reason"),
            rs.getString("trace_id"),
            toInstant(rs.getTimestamp("created_at")),
            toInstant(rs.getTimestamp("updated_at"))
    );

    private static final RowMapper<SagaHistoryEntry> HISTORY_MAPPER = (rs, rowNum) -> new SagaHistoryEntry(
            rs.getLong("id"),
            rs.getString("order_id"),
            rs.getString("from_status"),
            rs.getString("to_status"),
            rs.getString("trigger_event"),
            rs.getString("detail"),
            toInstant(rs.getTimestamp("occurred_at"))
    );

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertOrder(OrderRecord order) {
        jdbcTemplate.update(
                """
                INSERT INTO orders (
                    order_id, customer_id, customer_email, sku, quantity, total_amount_cents,
                    currency, payment_method_token, shipping_postal_code, coupon_code, status, trace_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                order.orderId(),
                order.customerId(),
                order.customerEmail(),
                order.sku(),
                order.quantity(),
                order.totalAmountCents(),
                order.currency(),
                order.paymentMethodToken(),
                order.shippingPostalCode(),
                order.couponCode(),
                order.status().name(),
                order.traceId()
        );
    }

    public Optional<OrderRecord> findById(String orderId) {
        List<OrderRecord> rows = jdbcTemplate.query(
                "SELECT * FROM orders WHERE order_id = ?",
                ORDER_MAPPER,
                orderId
        );
        return rows.stream().findFirst();
    }

    public Optional<OrderRecord> findByIdForUpdate(String orderId) {
        List<OrderRecord> rows = jdbcTemplate.query(
                "SELECT * FROM orders WHERE order_id = ? FOR UPDATE",
                ORDER_MAPPER,
                orderId
        );
        return rows.stream().findFirst();
    }

    public List<OrderRecord> findRecent(int limit) {
        return jdbcTemplate.query(
                "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?",
                ORDER_MAPPER,
                limit
        );
    }

    public void markPaymentAuthorized(String orderId, String paymentId) {
        jdbcTemplate.update(
                """
                UPDATE orders
                SET status = ?, payment_id = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                OrderStatus.PAYMENT_AUTHORIZED.name(),
                paymentId,
                orderId
        );
    }

    public void markStockReserved(String orderId, String reservationId) {
        jdbcTemplate.update(
                """
                UPDATE orders
                SET status = ?, reservation_id = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                OrderStatus.STOCK_RESERVED.name(),
                reservationId,
                orderId
        );
    }

    public void markCompleted(String orderId, String shipmentId, String trackingNumber) {
        jdbcTemplate.update(
                """
                UPDATE orders
                SET status = ?, shipment_id = ?, tracking_number = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                OrderStatus.COMPLETED.name(),
                shipmentId,
                trackingNumber,
                orderId
        );
    }

    public void markCancelled(String orderId, String failureStage, String failureReason) {
        jdbcTemplate.update(
                """
                UPDATE orders
                SET status = ?, failure_stage = ?, failure_reason = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                OrderStatus.CANCELLED.name(),
                failureStage,
                failureReason,
                orderId
        );
    }

    public void appendHistory(
            String orderId,
            String fromStatus,
            String toStatus,
            String triggerEvent,
            String detail) {
        jdbcTemplate.update(
                """
                INSERT INTO order_saga_history (order_id, from_status, to_status, trigger_event, detail)
                VALUES (?, ?, ?, ?, ?)
                """,
                orderId,
                fromStatus,
                toStatus,
                triggerEvent,
                detail
        );
    }

    public List<SagaHistoryEntry> findHistory(String orderId) {
        return jdbcTemplate.query(
                "SELECT * FROM order_saga_history WHERE order_id = ? ORDER BY id ASC",
                HISTORY_MAPPER,
                orderId
        );
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
