package io.orderplatform.common;

import io.orderplatform.common.kafka.AvroOrRawBytesDeserializer;
import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.common.kafka.PlatformTopicsConfiguration;
import io.orderplatform.events.TopicNames;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlatformTopicsAndSerdeTest {

    @Test
    void topicsAsCodeDeclaresSixPartitionsRf3AndMinIsr2ForAllDomainRetryAndDltTopics() {
        PlatformKafkaProperties props = new PlatformKafkaProperties();
        List<NewTopic> topics = PlatformTopicsConfiguration.buildAllTopics(props);

        // 11 domain topics * (1 base + 3 retry + 1 dlt) = 55 topics
        assertThat(topics).hasSize(TopicNames.ALL_DOMAIN_TOPICS.size() * 5);
        for (NewTopic topic : topics) {
            assertThat(topic.numPartitions()).isEqualTo(6);
            assertThat(topic.replicationFactor()).isEqualTo((short) 3);
            assertThat(topic.configs()).containsEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        }
    }

    @Test
    void producerAndConsumerConfigsEnforceIdempotenceKip848ManualCommitsAndReadCommitted() {
        PlatformKafkaProperties props = new PlatformKafkaProperties();
        Map<String, Object> producerCfg = KafkaSerdeFactory.avroProducerConfigs(props);
        assertThat(producerCfg).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        assertThat(producerCfg).containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        Map<String, Object> consumerCfg = KafkaSerdeFactory.avroConsumerConfigs(props, "inventory-service-group");
        assertThat(consumerCfg).containsEntry(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        assertThat(consumerCfg).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        assertThat(consumerCfg).containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        assertThat(consumerCfg).containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        assertThat(consumerCfg).containsEntry(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, AvroOrRawBytesDeserializer.class);
    }

    @Test
    void avroOrRawBytesDeserializerRejectsCorruptBytesOnMainTopicAndPreservesRawBytesOnDlt() {
        PlatformKafkaProperties props = new PlatformKafkaProperties();
        try (AvroOrRawBytesDeserializer deserializer = new AvroOrRawBytesDeserializer()) {
            deserializer.configure(KafkaSerdeFactory.avroConsumerConfigs(props, "serde-test"), false);
            byte[] corrupt = "CORRUPT_WIRE_BYTES".getBytes(StandardCharsets.UTF_8);

            assertThatThrownBy(() -> deserializer.deserialize(TopicNames.INVENTORY_RESERVED, corrupt))
                    .isInstanceOf(RuntimeException.class);

            Object dltDecoded = deserializer.deserialize(TopicNames.dltTopic(TopicNames.INVENTORY_RESERVED), corrupt);
            assertThat(dltDecoded).isEqualTo(corrupt);
        }
    }
}
