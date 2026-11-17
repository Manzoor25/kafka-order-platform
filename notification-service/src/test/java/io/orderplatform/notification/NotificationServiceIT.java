package io.orderplatform.notification;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.PaymentRefunded;
import io.orderplatform.notification.domain.NotificationRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class NotificationServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("notification_db")
            .withUsername("notification_user")
            .withPassword("notification_pass");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static final GenericContainer<?> APICURIO = new GenericContainer<>("apicurio/apicurio-registry:3.3.4")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200));

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("platform.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("platform.kafka.schema-registry-url",
                () -> "http://" + APICURIO.getHost() + ":" + APICURIO.getMappedPort(8080) + "/apis/registry/v3");
        registry.add("platform.kafka.topic-replicas", () -> 1);
        registry.add("platform.kafka.topic-min-insync-replicas", () -> 1);
        registry.add("management.tracing.export.otlp.enabled", () -> false);
    }

    @Autowired
    private KafkaTemplate<String, Object> notificationKafkaTemplate;

    @Autowired
    private NotificationRepository notificationRepository;

    @Autowired
    private Tracer notificationTracer;

    @Test
    void consumesLifecycleEventsIdempotentlyAndPreservesDistributedTraceId() {
        String orderId = "ord-ntf-" + UUID.randomUUID();
        String eventId = UUID.randomUUID().toString();
        OrderConfirmed confirmed = new OrderConfirmed(
                eventId, orderId, "cust-1", "customer@example.com",
                "ship-1", "DHL-98101-9999", 18900L, "USD", Instant.now().toEpochMilli());

        String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String traceparent = "00-" + traceId + "-00f067aa0ba902b7-01";
        try (Scope ignored = KafkaTracePropagator.extractFromTraceparent(traceparent).makeCurrent()) {
            ProducerRecord<String, Object> rec = KafkaTracePropagator.tracedProducerRecord(
                    notificationTracer, TopicNames.ORDER_CONFIRMED, orderId, confirmed, traceparent);
            // Send duplicate event twice; processed_events ensures only 1 notification is recorded
            notificationKafkaTemplate.send(rec);
            notificationKafkaTemplate.send(rec);
        }

        PaymentRefunded refunded = new PaymentRefunded(
                UUID.randomUUID().toString(), orderId, "pay-1", "re_1",
                18900L, "USD", "SHIPPING_FAILED", "Address rejected",
                "customer@example.com", Instant.now().toEpochMilli());
        notificationKafkaTemplate.send(TopicNames.PAYMENT_REFUNDED, orderId, refunded);

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var entries = notificationRepository.findByOrderId(orderId);
            assertThat(entries).hasSize(2);
            assertThat(entries).anySatisfy(e -> {
                assertThat(e.notificationType()).isEqualTo("ORDER_CONFIRMED");
                assertThat(e.traceId()).isEqualTo(traceId);
            });
            assertThat(entries).anySatisfy(e ->
                    assertThat(e.notificationType()).isEqualTo("PAYMENT_REFUNDED"));
        });
    }
}
