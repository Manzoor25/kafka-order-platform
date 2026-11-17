package io.orderplatform.order;

import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.OrderConfirmed;
import io.orderplatform.events.avro.OrderCreated;
import io.orderplatform.events.avro.ShipmentCreated;
import io.orderplatform.order.api.CreateOrderRequest;
import io.orderplatform.order.api.OrderDetailResponse;
import io.orderplatform.order.outbox.OutboxRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderServiceIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine")
            .withDatabaseName("order_db")
            .withUsername("order_user")
            .withPassword("order_pass");

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
        registry.add("order.outbox.poll-interval-ms", () -> 100);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private KafkaTemplate<String, Object> orderKafkaTemplate;

    @Autowired
    private PlatformKafkaProperties kafkaProperties;

    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    void transactionalOutboxPublishesOrderCreatedAndCompletesWhenShipmentCreatedArrives() {
        RestClient client = RestClient.create("http://localhost:" + port);
        String orderId = "ord-it-" + UUID.randomUUID();

        ConsumerFactory<String, Object> verifyFactory = KafkaSerdeFactory.avroConsumerFactory(
                kafkaProperties, "order-it-verifier-" + UUID.randomUUID());
        try (Consumer<String, Object> consumer = verifyFactory.createConsumer()) {
            consumer.subscribe(List.of(TopicNames.ORDER_CREATED, TopicNames.ORDER_CONFIRMED));

            OrderDetailResponse created = client.post()
                    .uri("/api/orders")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new CreateOrderRequest(
                            orderId, "cust-it", "it@example.com", "SKU-HEADSET",
                            2, 19800L, "USD", "tok_visa_ok", "98101", "LAUNCH20"))
                    .retrieve()
                    .body(OrderDetailResponse.class);

            assertThat(created).isNotNull();
            assertThat(created.status()).isEqualTo("PENDING");
            assertThat(created.traceId()).isNotBlank();

            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(outboxRepository.findByAggregateId(orderId))
                            .anySatisfy(row -> assertThat(row.publishedAt()).isNotNull()));

            ConsumerRecord<String, Object> orderCreatedRecord = pollForOrder(consumer, TopicNames.ORDER_CREATED, orderId);
            assertThat(orderCreatedRecord.key()).isEqualTo(orderId);
            assertThat(orderCreatedRecord.value()).isInstanceOf(OrderCreated.class);
            assertThat(KafkaTracePropagator.extractTraceparentHeader(orderCreatedRecord.headers()))
                    .contains(created.traceId());

            ShipmentCreated shipment = ShipmentCreated.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOrderId(orderId)
                    .setShipmentId("ship-" + UUID.randomUUID())
                    .setCarrier("DHL")
                    .setTrackingNumber("DHL-98101-OK")
                    .setShippingPostalCode("98101")
                    .setCustomerEmail("it@example.com")
                    .setOccurredAtEpochMs(Instant.now().toEpochMilli())
                    .build();
            orderKafkaTemplate.send(TopicNames.SHIPPING_SCHEDULED, orderId, shipment);

            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                OrderDetailResponse updated = client.get()
                        .uri("/api/orders/{id}", orderId)
                        .retrieve()
                        .body(OrderDetailResponse.class);
                assertThat(updated).isNotNull();
                assertThat(updated.status()).isEqualTo("COMPLETED");
                assertThat(updated.trackingNumber()).isEqualTo("DHL-98101-OK");
            });

            ConsumerRecord<String, Object> confirmedRecord = pollForOrder(consumer, TopicNames.ORDER_CONFIRMED, orderId);
            assertThat(confirmedRecord.value()).isInstanceOf(OrderConfirmed.class);
        }
    }

    private static ConsumerRecord<String, Object> pollForOrder(
            Consumer<String, Object> consumer, String expectedTopic, String orderId) {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(400));
            for (ConsumerRecord<String, Object> rec : records.records(expectedTopic)) {
                if (orderId.equals(rec.key())) {
                    return rec;
                }
            }
        }
        throw new AssertionError("Timed out waiting for " + expectedTopic + " record with key=" + orderId);
    }
}
