package io.orderplatform.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroSerdeConfig;
import io.apicurio.registry.serde.avro.strategy.RecordIdStrategy;
import io.apicurio.registry.serde.config.IdOption;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.orderplatform.common.kafka.KafkaSerdeFactory;
import io.orderplatform.common.kafka.PlatformKafkaProperties;
import io.orderplatform.events.AvroSecurityConfig;
import io.orderplatform.events.avro.InventoryReserved;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Core inspector and replay engine for dead-letter topics ({@code *.dlt}).
 * Safely reads mixed Avro and corrupt raw-byte records up to the high-watermark,
 * strips stale {@code kafka_dlt-*}/{@code retry_topic-*} headers on replay, preserves W3C
 * {@code traceparent}, and attaches {@code x-dlq-replayed-at}.
 */
public class DeadLetterInspectorAndReplayer {

    public static final String REPLAYED_AT_HEADER = "x-dlq-replayed-at";
    public static final String REPLAY_SOURCE_TOPIC_HEADER = "x-dlq-source-topic";
    public static final String REPLAY_SOURCE_OFFSET_HEADER = "x-dlq-source-offset";

    static {
        AvroSecurityConfig.registerTrustedPackages();
    }

    public record DeadLetterEntry(
            String dltTopic,
            int partition,
            long offset,
            long timestampEpochMs,
            String key,
            boolean validAvro,
            String schemaType,
            String payloadSummary,
            String originalTopic,
            String exceptionClass,
            String exceptionMessage,
            String traceparent,
            SpecificRecord decodedRecord,
            byte[] rawBytes
    ) {
    }

    public record ReplayResult(
            int inspectedCount,
            int replayedCount,
            int skippedCorruptCount,
            List<String> replayedKeys
    ) {
    }

    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public List<DeadLetterEntry> inspect(DlqCliOptions options) {
        List<DeadLetterEntry> entries = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = createRawDltConsumer(options.bootstrapServers());
             AvroKafkaDeserializer<Object> avroDeserializer = createAvroDeserializer(options.schemaRegistryUrl())) {

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(options.topic(), Duration.ofSeconds(10));
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return entries;
            }
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(options.topic(), pi.partition()))
                    .filter(tp -> options.partitionFilter() == null || options.partitionFilter() == tp.partition())
                    .toList();
            if (partitions.isEmpty()) {
                return entries;
            }

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, Duration.ofSeconds(10));

            Map<TopicPartition, Long> remainingTargets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long end = endOffsets.getOrDefault(tp, 0L);
                if (end > 0) {
                    remainingTargets.put(tp, end);
                }
            }

            Instant deadline = Instant.now().plusSeconds(15);
            while (!remainingTargets.isEmpty() && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(400));
                for (ConsumerRecord<String, byte[]> rec : batch) {
                    TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                    Long targetEnd = remainingTargets.get(tp);
                    if (targetEnd == null || rec.offset() >= targetEnd) {
                        remainingTargets.remove(tp);
                        continue;
                    }
                    if (matchesFilters(rec, options)) {
                        entries.add(decodeRecord(rec, avroDeserializer));
                    }
                    if (rec.offset() + 1 >= targetEnd) {
                        remainingTargets.remove(tp);
                    }
                }
                for (TopicPartition tp : new ArrayList<>(remainingTargets.keySet())) {
                    if (consumer.position(tp) >= remainingTargets.get(tp)) {
                        remainingTargets.remove(tp);
                    }
                }
            }
        }
        return entries;
    }

    public ReplayResult replay(DlqCliOptions options) {
        List<DeadLetterEntry> entries = inspect(options);
        if (entries.isEmpty()) {
            return new ReplayResult(0, 0, 0, List.of());
        }

        PlatformKafkaProperties props = new PlatformKafkaProperties();
        props.setBootstrapServers(options.bootstrapServers());
        props.setSchemaRegistryUrl(options.schemaRegistryUrl());

        int replayed = 0;
        int skippedCorrupt = 0;
        List<String> keys = new ArrayList<>();

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(KafkaSerdeFactory.avroProducerConfigs(props))) {
            for (DeadLetterEntry entry : entries) {
                if (!entry.validAvro() && !options.replayRawCorrupt()) {
                    skippedCorrupt++;
                    continue;
                }
                String destinationTopic = resolveTargetTopic(options, entry);
                Object payload = prepareReplayPayload(entry, options);

                ProducerRecord<String, Object> out = new ProducerRecord<>(destinationTopic, entry.key(), payload);
                populateReplayHeaders(out.headers(), entry);

                if (!options.dryRun()) {
                    try {
                        producer.send(out).get(10, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        throw new IllegalStateException(
                                "Failed replaying key=" + entry.key() + " to " + destinationTopic, ex);
                    }
                }
                replayed++;
                keys.add(entry.key() == null ? "<null>" : entry.key());
            }
        }
        return new ReplayResult(entries.size(), replayed, skippedCorrupt, keys);
    }

    public String formatInspectionOutput(List<DeadLetterEntry> entries, String format) throws Exception {
        if ("json".equalsIgnoreCase(format)) {
            List<Map<String, Object>> serializable = entries.stream().map(e -> {
                Map<String, Object> m = new HashMap<>();
                m.put("dltTopic", e.dltTopic());
                m.put("partition", e.partition());
                m.put("offset", e.offset());
                m.put("key", e.key());
                m.put("validAvro", e.validAvro());
                m.put("schemaType", e.schemaType());
                m.put("originalTopic", e.originalTopic());
                m.put("exceptionClass", e.exceptionClass());
                m.put("exceptionMessage", e.exceptionMessage());
                m.put("traceparent", e.traceparent());
                m.put("payload", e.payloadSummary());
                return m;
            }).toList();
            return objectMapper.writeValueAsString(serializable);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== Dead-Letter Queue Inspection (").append(entries.size()).append(" record(s)) ===\n");
        for (DeadLetterEntry e : entries) {
            sb.append(String.format(
                    "[%s p=%d off=%d] key=%s type=%s origTopic=%s trace=%s%n  error: %s (%s)%n  payload: %s%n",
                    e.dltTopic(), e.partition(), e.offset(), e.key(), e.schemaType(),
                    e.originalTopic(), e.traceparent(), e.exceptionMessage(), e.exceptionClass(),
                    e.payloadSummary()));
        }
        return sb.toString();
    }

    static String resolveTargetTopic(DlqCliOptions options, DeadLetterEntry entry) {
        if (options.targetTopic() != null && !options.targetTopic().isBlank()) {
            return options.targetTopic().trim();
        }
        if (entry.originalTopic() != null && !entry.originalTopic().isBlank()) {
            return entry.originalTopic().replaceFirst("\\.retry-\\d+$", "");
        }
        return entry.dltTopic().replaceFirst("\\.dlt$", "");
    }

    static Object prepareReplayPayload(DeadLetterEntry entry, DlqCliOptions options) {
        if (!entry.validAvro()) {
            return entry.rawBytes();
        }
        SpecificRecord record = entry.decodedRecord();
        if (options.overridePostalCode() != null
                && !options.overridePostalCode().isBlank()
                && record instanceof InventoryReserved inv) {
            return InventoryReserved.newBuilder(inv)
                    .setShippingPostalCode(options.overridePostalCode().trim())
                    .build();
        }
        return record;
    }

    static void populateReplayHeaders(Headers targetHeaders, DeadLetterEntry entry) {
        if (entry.traceparent() != null && !entry.traceparent().isBlank()) {
            targetHeaders.add("traceparent", entry.traceparent().getBytes(StandardCharsets.UTF_8));
        }
        targetHeaders.add(REPLAYED_AT_HEADER, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        targetHeaders.add(REPLAY_SOURCE_TOPIC_HEADER, entry.dltTopic().getBytes(StandardCharsets.UTF_8));
        targetHeaders.add(REPLAY_SOURCE_OFFSET_HEADER,
                String.valueOf(entry.offset()).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean matchesFilters(ConsumerRecord<String, byte[]> rec, DlqCliOptions options) {
        if (options.keyFilter() != null && !options.keyFilter().equals(rec.key())) {
            return false;
        }
        return options.offsetFilter() == null || options.offsetFilter() == rec.offset();
    }

    private static DeadLetterEntry decodeRecord(
            ConsumerRecord<String, byte[]> rec,
            AvroKafkaDeserializer<Object> avroDeserializer) {
        String origTopic = headerUtf8(rec.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC);
        String exClass = headerUtf8(rec.headers(), KafkaHeaders.DLT_EXCEPTION_FQCN);
        String exMsg = headerUtf8(rec.headers(), KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String traceparent = headerUtf8(rec.headers(), "traceparent");

        byte[] raw = rec.value();
        if (raw != null && raw.length > 5 && raw[0] == 0x00) {
            try {
                Object decoded = avroDeserializer.deserialize(rec.topic(), rec.headers(), raw);
                if (decoded instanceof SpecificRecord specific) {
                    return new DeadLetterEntry(
                            rec.topic(), rec.partition(), rec.offset(), rec.timestamp(),
                            rec.key(), true, specific.getSchema().getFullName(),
                            specific.toString(), origTopic, exClass, exMsg, traceparent, specific, raw
                    );
                }
            } catch (Exception ignored) {
                // Fall through to raw corrupt bytes representation
            }
        }

        String preview = raw == null
                ? "<null>"
                : "hex=" + HexFormat.of().formatHex(raw) + " utf8=" + new String(raw, StandardCharsets.UTF_8);
        return new DeadLetterEntry(
                rec.topic(), rec.partition(), rec.offset(), rec.timestamp(),
                rec.key(), false, "RAW_CORRUPT_BYTES", preview,
                origTopic, exClass, exMsg, traceparent, null, raw
        );
    }

    private static String headerUtf8(Headers headers, String name) {
        Header h = headers.lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static KafkaConsumer<String, byte[]> createRawDltConsumer(String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-cli-inspector-" + UUID.randomUUID());
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(cfg);
    }

    private static AvroKafkaDeserializer<Object> createAvroDeserializer(String schemaRegistryUrl) {
        AvroKafkaDeserializer<Object> deserializer = new AvroKafkaDeserializer<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SchemaResolverConfig.REGISTRY_URL, schemaRegistryUrl);
        cfg.put(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, RecordIdStrategy.class.getName());
        cfg.put(KafkaSerdeConfig.ENABLE_HEADERS, false);
        cfg.put(SerdeConfig.USE_ID, IdOption.contentId.name());
        cfg.put(AvroSerdeConfig.USE_SPECIFIC_AVRO_READER, true);
        deserializer.configure(cfg, false);
        return deserializer;
    }
}
