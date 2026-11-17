package io.orderplatform.notification;

import io.orderplatform.common.idempotency.ProcessedEventStore;
import io.orderplatform.events.avro.OrderCancelled;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.notification.domain.CustomerNotificationService;
import io.orderplatform.notification.domain.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CustomerNotificationServiceTest {

    private NotificationRepository repository;
    private ProcessedEventStore processedEventStore;
    private CustomerNotificationService service;

    @BeforeEach
    void setUp() {
        repository = mock(NotificationRepository.class);
        processedEventStore = mock(ProcessedEventStore.class);
        service = new CustomerNotificationService(repository, processedEventStore);
    }

    @Test
    void orderConfirmedPersistsCustomerNotificationOnce() {
        when(processedEventStore.tryClaim(eq("evt-c1"), eq("OrderConfirmed"), eq("ord-1"), anyString()))
                .thenReturn(true);

        service.onOrderConfirmed(new OrderConfirmed(
                "evt-c1", "ord-1", "cust-1", "alice@example.com",
                "ship-1", "UPS-98101-XYZ", 14900L, "USD", Instant.now().toEpochMilli()));

        verify(repository).insertIdempotently(
                anyString(), eq("ORDER_CONFIRMED:ord-1"), eq("ord-1"),
                eq("alice@example.com"), eq("ORDER_CONFIRMED"),
                contains("ord-1"), contains("UPS-98101-XYZ"), any());
    }

    @Test
    void duplicateOrderCancelledEventIsIgnored() {
        when(processedEventStore.tryClaim(eq("evt-dup"), eq("OrderCancelled"), eq("ord-2"), anyString()))
                .thenReturn(false);

        service.onOrderCancelled(new OrderCancelled(
                "evt-dup", "ord-2", "cust-2", "bob@example.com",
                "PAYMENT_DECLINED", "Card declined", Instant.now().toEpochMilli()));

        verify(repository, never()).insertIdempotently(
                any(), any(), any(), any(), any(), any(), any(), any());
    }
}
