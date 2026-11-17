package io.orderplatform.dlq;

import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DlqCliIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

    @Container
    static final GenericContainer<?> APICURIO = new GenericContainer<>("apicurio/apicurio-registry:3.3.4")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200));

    @Test
    void inspectsMixedAvroAndCorruptDltRecordsAndReplaysRepairedAvroToOriginTopic() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        String registryUrl = "http://" + APICURIO.getHost() + ":" + APICURIO.getMappedPort(8080) + "/apis/registry/v3";
        String baseTopic = TopicNames.INVENTORY_RESERVED;
        String dltTopic = TopicNames.dltTopic(baseTopic);

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(baseTopic, 3, (short) 1), new NewTopic(dltTopic, 3, (short) 1)))
                    .all().get();
        }

        PlatformKafkaProperties props = new PlatformKafkaProperties();
        props.setBootstrapServers(bootstrap);
        props.setSchemaRegistryUrl(registryUrl);

        String orderId = "ord-dlq-it-" + UUID.randomUUID();
        InventoryReserved poison = new InventoryReserved(
                UUID.randomUUID().toString(), orderId, "res-1", "SKU-HEADSET", 1,
                "POISON-ALWAYS", "dlq@example.com", Instant.now().toEpochMilli());

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(KafkaSerdeFactory.avroProducerConfigs(props))) {
            ProducerRecord<String, Object> dltAvro = new ProducerRecord<>(dltTopic, orderId, poison);
            dltAvro.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC,
                    (baseTopic + ".retry-2").getBytes(StandardCharsets.UTF_8));
            dltAvro.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE,
                    "Carrier 503".getBytes(StandardCharsets.UTF_8));
            dltAvro.headers().add("traceparent",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes(StandardCharsets.UTF_8));
            producer.send(dltAvro).get();

            ProducerRecord<String, Object> dltCorrupt = new ProducerRecord<>(
                    dltTopic, "ord-corrupt", "BAD_RAW_BYTES".getBytes(StandardCharsets.UTF_8));
            producer.send(dltCorrupt).get();
        }

        DeadLetterInspectorAndReplayer engine = new DeadLetterInspectorAndReplayer();
        var inspected = engine.inspect(DlqCliOptions.parse(new String[]{
                "inspect", "--topic", dltTopic,
                "--bootstrap-servers", bootstrap,
                "--schema-registry-url", registryUrl
        }));
        assertThat(inspected).hasSize(2);

        var replayResult = engine.replay(DlqCliOptions.parse(new String[]{
                "replay", "--topic", dltTopic,
                "--key", orderId,
                "--override-postal-code", "98101",
                "--bootstrap-servers", bootstrap,
                "--schema-registry-url", registryUrl
        }));
        assertThat(replayResult.replayedCount()).isEqualTo(1);

        ConsumerFactory<String, Object> cf = KafkaSerdeFactory.avroConsumerFactory(props, "dlq-it-verify");
        try (Consumer<String, Object> consumer = cf.createConsumer()) {
            consumer.subscribe(List.of(baseTopic));
            Instant deadline = Instant.now().plusSeconds(10);
            ConsumerRecord<String, Object> replayedRecord = null;
            while (Instant.now().isBefore(deadline) && replayedRecord == null) {
                ConsumerRecords<String, Object> recs = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, Object> r : recs.records(baseTopic)) {
                    if (orderId.equals(r.key())) {
                        replayedRecord = r;
                    }
                }
            }
            assertThat(replayedRecord).isNotNull();
            InventoryReserved repaired = (InventoryReserved) replayedRecord.value();
            assertThat(repaired.getShippingPostalCode()).isEqualTo("98101");
            assertThat(replayedRecord.headers().lastHeader(DeadLetterInspectorAndReplayer.REPLAYED_AT_HEADER)).isNotNull();
        }
    }
}
