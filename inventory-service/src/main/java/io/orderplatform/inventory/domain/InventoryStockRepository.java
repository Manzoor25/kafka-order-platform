package io.orderplatform.inventory.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class InventoryStockRepository {

    public record StockItem(String sku, String productName, int availableQty, int reservedQty) {
    }

    public record ReservationDecision(
            boolean reserved,
            String reservationId,
            String sku,
            int quantity,
            int availableQuantity,
            String status,
            String reason
    ) {
    }

    private final JdbcTemplate jdbcTemplate;

    public InventoryStockRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<StockItem> findStockBySku(String sku) {
        List<StockItem> rows = jdbcTemplate.query(
                "SELECT sku, product_name, available_qty, reserved_qty FROM stock_catalog WHERE sku = ?",
                (rs, rowNum) -> new StockItem(
                        rs.getString("sku"),
                        rs.getString("product_name"),
                        rs.getInt("available_qty"),
                        rs.getInt("reserved_qty")),
                sku
        );
        return rows.stream().findFirst();
    }

    @Transactional
    public ReservationDecision reserveIdempotently(String orderId, String sku, int requestedQty) {
        List<ReservationDecision> existing = jdbcTemplate.query(
                """
                SELECT reservation_id, sku, quantity, available_snapshot, status, reason
                FROM stock_reservations
                WHERE order_id = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> new ReservationDecision(
                        "RESERVED".equals(rs.getString("status")),
                        rs.getString("reservation_id"),
                        rs.getString("sku"),
                        rs.getInt("quantity"),
                        rs.getInt("available_snapshot"),
                        rs.getString("status"),
                        rs.getString("reason")
                ),
                orderId
        );
        if (!existing.isEmpty()) {
            return existing.getFirst();
        }

        String reservationId = "res-" + UUID.nameUUIDFromBytes(("res:" + orderId).getBytes(StandardCharsets.UTF_8));
        List<StockItem> lockedCatalog = jdbcTemplate.query(
                "SELECT sku, product_name, available_qty, reserved_qty FROM stock_catalog WHERE sku = ? FOR UPDATE",
                (rs, rowNum) -> new StockItem(
                        rs.getString("sku"),
                        rs.getString("product_name"),
                        rs.getInt("available_qty"),
                        rs.getInt("reserved_qty")),
                sku
        );

        if (lockedCatalog.isEmpty()) {
            String reason = "Unknown SKU: " + sku;
            jdbcTemplate.update(
                    """
                    INSERT INTO stock_reservations (
                        reservation_id, order_id, sku, quantity, available_snapshot, status, reason
                    ) VALUES (?, ?, ?, ?, 0, 'REJECTED', ?)
                    """,
                    reservationId, orderId, sku, requestedQty, reason
            );
            return new ReservationDecision(false, reservationId, sku, requestedQty, 0, "REJECTED", reason);
        }

        int available = lockedCatalog.getFirst().availableQty();
        if (available < requestedQty) {
            String reason = "Insufficient stock for " + sku + " (requested=" + requestedQty + ", available=" + available + ")";
            jdbcTemplate.update(
                    """
                    INSERT INTO stock_reservations (
                        reservation_id, order_id, sku, quantity, available_snapshot, status, reason
                    ) VALUES (?, ?, ?, ?, ?, 'REJECTED', ?)
                    """,
                    reservationId, orderId, sku, requestedQty, available, reason
            );
            return new ReservationDecision(false, reservationId, sku, requestedQty, available, "REJECTED", reason);
        }

        jdbcTemplate.update(
                """
                UPDATE stock_catalog
                SET available_qty = available_qty - ?, reserved_qty = reserved_qty + ?, updated_at = NOW()
                WHERE sku = ?
                """,
                requestedQty, requestedQty, sku
        );
        jdbcTemplate.update(
                """
                INSERT INTO stock_reservations (
                    reservation_id, order_id, sku, quantity, available_snapshot, status, reason
                ) VALUES (?, ?, ?, ?, ?, 'RESERVED', NULL)
                """,
                reservationId, orderId, sku, requestedQty, available - requestedQty
        );
        return new ReservationDecision(true, reservationId, sku, requestedQty, available - requestedQty, "RESERVED", null);
    }

    @Transactional
    public Optional<ReservationDecision> releaseIdempotently(String orderId, String reason) {
        List<ReservationDecision> rows = jdbcTemplate.query(
                """
                SELECT reservation_id, sku, quantity, available_snapshot, status, reason
                FROM stock_reservations
                WHERE order_id = ?
                FOR UPDATE
                """,
                (rs, rowNum) -> new ReservationDecision(
                        "RESERVED".equals(rs.getString("status")),
                        rs.getString("reservation_id"),
                        rs.getString("sku"),
                        rs.getInt("quantity"),
                        rs.getInt("available_snapshot"),
                        rs.getString("status"),
                        rs.getString("reason")
                ),
                orderId
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        ReservationDecision current = rows.getFirst();
        if ("RELEASED".equals(current.status())) {
            return Optional.of(current);
        }
        if (!"RESERVED".equals(current.status())) {
            return Optional.empty();
        }

        jdbcTemplate.update(
                """
                UPDATE stock_catalog
                SET available_qty = available_qty + ?, reserved_qty = GREATEST(0, reserved_qty - ?), updated_at = NOW()
                WHERE sku = ?
                """,
                current.quantity(), current.quantity(), current.sku()
        );
        jdbcTemplate.update(
                """
                UPDATE stock_reservations
                SET status = 'RELEASED', reason = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                reason, orderId
        );
        return Optional.of(new ReservationDecision(
                false, current.reservationId(), current.sku(), current.quantity(),
                current.availableQuantity() + current.quantity(), "RELEASED", reason
        ));
    }
}
