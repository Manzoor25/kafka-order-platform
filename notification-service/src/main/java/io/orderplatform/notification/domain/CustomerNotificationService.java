package io.orderplatform.notification.domain;

import io.opentelemetry.api.trace.Span;
import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.notification.config.NotificationKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class CustomerNotificationService {

    private static final Logger log = LoggerFactory.getLogger(CustomerNotificationService.class);

    private final NotificationRepository notificationRepository;
    private final ProcessedEventStore processedEventStore;

    public CustomerNotificationService(
            NotificationRepository notificationRepository,
            ProcessedEventStore processedEventStore) {
        this.notificationRepository = notificationRepository;
        this.processedEventStore = processedEventStore;
    }

    @Transactional
    public void onOrderConfirmed(OrderConfirmed event) {
        recordNotification(
                event.getEventId(),
                "OrderConfirmed",
                event.getOrderId(),
                event.getCustomerEmail(),
                "ORDER_CONFIRMED",
                "Order " + event.getOrderId() + " confirmed",
                "Your order is confirmed and shipped under tracking " + event.getTrackingNumber()
                        + " (" + event.getTotalAmountCents() + " " + event.getCurrency() + ")."
        );
    }

    @Transactional
    public void onOrderCancelled(OrderCancelled event) {
        recordNotification(
                event.getEventId(),
                "OrderCancelled",
                event.getOrderId(),
                event.getCustomerEmail(),
                "ORDER_CANCELLED",
                "Order " + event.getOrderId() + " cancelled",
                "Your order was cancelled at stage " + event.getFailedStage() + ": " + event.getReason()
        );
    }

    @Transactional
    public void onPaymentRefunded(PaymentRefunded event) {
        recordNotification(
                event.getEventId(),
                "PaymentRefunded",
                event.getOrderId(),
                event.getCustomerEmail(),
                "PAYMENT_REFUNDED",
                "Refund issued for order " + event.getOrderId(),
                "Refund " + event.getGatewayRefundId() + " for " + event.getAmountCents() + " "
                        + event.getCurrency() + " completed (" + event.getReason() + ")."
        );
    }

    @Transactional
    public void onShipmentCreated(ShipmentCreated event) {
        recordNotification(
                event.getEventId(),
                "ShipmentCreated",
                event.getOrderId(),
                event.getCustomerEmail(),
                "SHIPMENT_DISPATCHED",
                "Shipment booked via " + event.getCarrier(),
                "Carrier " + event.getCarrier() + " assigned tracking " + event.getTrackingNumber()
                        + " to postal code " + event.getShippingPostalCode() + "."
        );
    }

    private void recordNotification(
            String eventId,
            String eventType,
            String orderId,
            String recipientEmail,
            String notificationType,
            String subject,
            String body) {
        if (!processedEventStore.tryClaim(eventId, eventType, orderId, NotificationKafkaConfig.CONSUMER_GROUP)) {
            log.info("Skipping duplicate {} eventId={} orderId={}", eventType, eventId, orderId);
            return;
        }
        String currentTraceId = Span.current().getSpanContext().isValid()
                ? Span.current().getSpanContext().getTraceId()
                : null;
        String dedupKey = notificationType + ":" + orderId;
        notificationRepository.insertIdempotently(
                "ntf-" + UUID.randomUUID(),
                dedupKey,
                orderId,
                recipientEmail,
                notificationType,
                subject,
                body,
                currentTraceId
        );
        log.info("NOTIFICATION_SENT type={} orderId={} to={} traceId={}",
                notificationType, orderId, recipientEmail, currentTraceId);
    }
}
