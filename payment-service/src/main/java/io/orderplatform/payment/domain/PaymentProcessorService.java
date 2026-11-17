package io.orderplatform.payment.domain;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReleased;
import io.orderplatform.events.avro.InventoryReservationFailed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.PaymentAuthorized;
import io.orderplatform.events.avro.PaymentDeclined;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.payment.config.PaymentKafkaConfig;
import io.orderplatform.payment.domain.PaymentLedgerRepository.PaymentRecord;
import io.orderplatform.payment.gateway.SimulatedCardGateway;
import io.orderplatform.payment.gateway.SimulatedCardGateway.GatewayResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes payment authorization and compensating refunds inside Kafka's transactional
 * read-process-write scope while crossing the external card gateway boundary safely via
 * deterministic idempotency keys ({@code "charge:" + orderId}, {@code "refund:" + orderId}).
 */
@Service
public class PaymentProcessorService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorService.class);

    public static final String TOKEN_ABORT_MID_TX_ONCE = "tok_abort_mid_tx_once";
    public static final String TOKEN_HOLD_MID_TX = "tok_hold_mid_tx";

    private final SimulatedCardGateway cardGateway;
    private final PaymentLedgerRepository ledgerRepository;
    private final ProcessedEventStore processedEventStore;
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;
    private final TransactionTemplate dbTransactionTemplate;
    private final Tracer tracer;
    private final long midTxHoldMs;
    private final Set<String> simulatedMidTxAborts = ConcurrentHashMap.newKeySet();

    public PaymentProcessorService(
            SimulatedCardGateway cardGateway,
            PaymentLedgerRepository ledgerRepository,
            ProcessedEventStore processedEventStore,
            KafkaTemplate<String, Object> paymentTransactionalKafkaTemplate,
            org.springframework.transaction.PlatformTransactionManager transactionManager,
            Tracer tracer,
            @Value("${payment.fault-injection.mid-tx-hold-ms:12000}") long midTxHoldMs) {
        this.cardGateway = cardGateway;
        this.ledgerRepository = ledgerRepository;
        this.processedEventStore = processedEventStore;
        this.transactionalKafkaTemplate = paymentTransactionalKafkaTemplate;
        this.dbTransactionTemplate = new TransactionTemplate(transactionManager);
        this.tracer = tracer;
        this.midTxHoldMs = midTxHoldMs;
    }

    public void processOrderCreated(OrderCreated event) {
        if (processedEventStore.isProcessed(event.getEventId())) {
            log.info("Skipping already processed OrderCreated eventId={} orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        // Step 1: External side effect (commits outside Kafka's transaction coordinator)
        String idempotencyKey = "charge:" + event.getOrderId();
        GatewayResult gatewayResult = cardGateway.authorize(
                idempotencyKey,
                event.getOrderId(),
                event.getTotalAmountCents(),
                event.getCurrency(),
                event.getPaymentMethodToken()
        );

        String paymentId = ledgerRepository.findByOrderId(event.getOrderId())
                .map(PaymentRecord::paymentId)
                .orElseGet(() -> "pay-" + UUID.nameUUIDFromBytes(event.getOrderId().getBytes()));

        // Step 2: Stage uncommitted outbound record inside the active Kafka transaction
        if (gatewayResult.approved()) {
            PaymentAuthorized authorized = PaymentAuthorized.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(event.getOrderId())
                    .setPaymentId(paymentId)
                    .setGatewayChargeId(gatewayResult.referenceId())
                    .setSku(event.getSku())
                    .setQuantity(event.getQuantity())
                    .setAmountCents(event.getTotalAmountCents())
                    .setCurrency(event.getCurrency())
                    .setShippingPostalCode(event.getShippingPostalCode())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            ProducerRecord<String, Object> outRecord = KafkaTracePropagator.tracedProducerRecord(
                    tracer, TopicNames.PAYMENT_AUTHORIZED, event.getOrderId(), authorized);
            transactionalKafkaTemplate.send(outRecord);
            transactionalKafkaTemplate.flush();

            maybeInjectMidTransactionCrash(event, gatewayResult);

            dbTransactionTemplate.executeWithoutResult(status -> {
                if (processedEventStore.tryClaim(
                        event.getEventId(), "OrderCreated", event.getOrderId(), PaymentKafkaConfig.CONSUMER_GROUP)) {
                    ledgerRepository.upsertAuthorized(
                            paymentId,
                            event.getOrderId(),
                            event.getCustomerEmail(),
                            event.getTotalAmountCents(),
                            event.getCurrency(),
                            gatewayResult.referenceId()
                    );
                }
            });
        } else {
            PaymentDeclined declined = PaymentDeclined.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(event.getOrderId())
                    .setPaymentId(paymentId)
                    .setAmountCents(event.getTotalAmountCents())
                    .setCurrency(event.getCurrency())
                    .setReason(gatewayResult.reason())
                    .setCustomerEmail(event.getCustomerEmail())
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            ProducerRecord<String, Object> outRecord = KafkaTracePropagator.tracedProducerRecord(
                    tracer, TopicNames.PAYMENT_DECLINED, event.getOrderId(), declined);
            transactionalKafkaTemplate.send(outRecord);
            transactionalKafkaTemplate.flush();

            dbTransactionTemplate.executeWithoutResult(status -> {
                if (processedEventStore.tryClaim(
                        event.getEventId(), "OrderCreated", event.getOrderId(), PaymentKafkaConfig.CONSUMER_GROUP)) {
                    ledgerRepository.upsertDeclined(
                            paymentId,
                            event.getOrderId(),
                            event.getCustomerEmail(),
                            event.getTotalAmountCents(),
                            event.getCurrency(),
                            gatewayResult.reason()
                    );
                }
            });
        }
    }

    public void compensateOnInventoryRejected(InventoryReservationFailed event) {
        compensateRefund(event.getEventId(), "InventoryReservationFailed",
                event.getOrderId(), "INVENTORY_REJECTED", event.getReason());
    }

    public void compensateOnInventoryReleased(InventoryReleased event) {
        compensateRefund(event.getEventId(), "InventoryReleased",
                event.getOrderId(), "SHIPPING_FAILED", event.getReason());
    }

    private void compensateRefund(
            String triggerEventId,
            String triggerEventType,
            String orderId,
            String compensationTrigger,
            String reason) {
        if (processedEventStore.isProcessed(triggerEventId)) {
            return;
        }
        PaymentRecord payment = ledgerRepository.findByOrderId(orderId).orElse(null);
        if (payment == null || "DECLINED".equals(payment.status())) {
            dbTransactionTemplate.executeWithoutResult(status ->
                    processedEventStore.tryClaim(triggerEventId, triggerEventType, orderId, PaymentKafkaConfig.CONSUMER_GROUP));
            return;
        }

        GatewayResult refundResult = cardGateway.refund(
                "refund:" + orderId,
                orderId,
                payment.amountCents(),
                payment.currency(),
                reason
        );

        PaymentRefunded refunded = PaymentRefunded.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setPaymentId(payment.paymentId())
                .setGatewayRefundId(refundResult.referenceId())
                .setAmountCents(payment.amountCents())
                .setCurrency(payment.currency())
                .setCompensationTrigger(compensationTrigger)
                .setReason(reason)
                .setCustomerEmail(payment.customerEmail())
                .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                .build();

        ProducerRecord<String, Object> outRecord = KafkaTracePropagator.tracedProducerRecord(
                tracer, TopicNames.PAYMENT_REFUNDED, orderId, refunded);
        transactionalKafkaTemplate.send(outRecord);
        transactionalKafkaTemplate.flush();

        dbTransactionTemplate.executeWithoutResult(status -> {
            if (processedEventStore.tryClaim(
                    triggerEventId, triggerEventType, orderId, PaymentKafkaConfig.CONSUMER_GROUP)) {
                ledgerRepository.markRefunded(orderId, refundResult.referenceId(), reason);
            }
        });
    }

    private void maybeInjectMidTransactionCrash(OrderCreated event, GatewayResult gatewayResult) {
        String token = event.getPaymentMethodToken();
        if (TOKEN_ABORT_MID_TX_ONCE.equals(token) && simulatedMidTxAborts.add(event.getOrderId())) {
            log.warn("MID_TX_ABORT_INJECTED orderId={} gatewayChargeId={} (card charged BEFORE Kafka TX commit!)",
                    event.getOrderId(), gatewayResult.referenceId());
            throw new IllegalStateException("Simulated payment-service crash mid-Kafka-transaction after card charge");
        }
        if (TOKEN_HOLD_MID_TX.equals(token) && !gatewayResult.deduplicatedFromPriorCall() && midTxHoldMs > 0) {
            log.warn("MID_TX_HOLD_READY orderId={} gatewayChargeId={} holdingOpenKafkaTxMs={}",
                    event.getOrderId(), gatewayResult.referenceId(), midTxHoldMs);
            try {
                Thread.sleep(midTxHoldMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted mid-Kafka-transaction", ie);
            }
        }
    }
}
