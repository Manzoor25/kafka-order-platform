package io.orderplatform.payment.domain;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class PaymentLedgerRepository {

    public record PaymentRecord(
            String paymentId,
            String orderId,
            String customerEmail,
            long amountCents,
            String currency,
            String status,
            String gatewayChargeId,
            String gatewayRefundId,
            String failureReason
    ) {
    }

    private static final RowMapper<PaymentRecord> MAPPER = (rs, rowNum) -> new PaymentRecord(
            rs.getString("payment_id"),
            rs.getString("order_id"),
            rs.getString("customer_email"),
            rs.getLong("amount_cents"),
            rs.getString("currency"),
            rs.getString("status"),
            rs.getString("gateway_charge_id"),
            rs.getString("gateway_refund_id"),
            rs.getString("failure_reason")
    );

    private final JdbcTemplate jdbcTemplate;

    public PaymentLedgerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<PaymentRecord> findByOrderId(String orderId) {
        List<PaymentRecord> rows = jdbcTemplate.query(
                "SELECT * FROM payments WHERE order_id = ?",
                MAPPER,
                orderId
        );
        return rows.stream().findFirst();
    }

    public void upsertAuthorized(
            String paymentId,
            String orderId,
            String customerEmail,
            long amountCents,
            String currency,
            String gatewayChargeId) {
        jdbcTemplate.update(
                """
                INSERT INTO payments (
                    payment_id, order_id, customer_email, amount_cents, currency, status, gateway_charge_id
                ) VALUES (?, ?, ?, ?, ?, 'AUTHORIZED', ?)
                ON CONFLICT (order_id) DO UPDATE
                SET status = 'AUTHORIZED', gateway_charge_id = EXCLUDED.gateway_charge_id, updated_at = NOW()
                """,
                paymentId, orderId, customerEmail, amountCents, currency, gatewayChargeId
        );
    }

    public void upsertDeclined(
            String paymentId,
            String orderId,
            String customerEmail,
            long amountCents,
            String currency,
            String reason) {
        jdbcTemplate.update(
                """
                INSERT INTO payments (
                    payment_id, order_id, customer_email, amount_cents, currency, status, failure_reason
                ) VALUES (?, ?, ?, ?, ?, 'DECLINED', ?)
                ON CONFLICT (order_id) DO UPDATE
                SET status = 'DECLINED', failure_reason = EXCLUDED.failure_reason, updated_at = NOW()
                """,
                paymentId, orderId, customerEmail, amountCents, currency, reason
        );
    }

    public void markRefunded(String orderId, String gatewayRefundId, String reason) {
        jdbcTemplate.update(
                """
                UPDATE payments
                SET status = 'REFUNDED', gateway_refund_id = ?, failure_reason = ?, updated_at = NOW()
                WHERE order_id = ?
                """,
                gatewayRefundId, reason, orderId
        );
    }
}
