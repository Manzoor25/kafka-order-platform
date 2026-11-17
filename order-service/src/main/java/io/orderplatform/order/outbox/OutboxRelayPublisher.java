package io.orderplatform.order.outbox;

import io.opentelemetry.api.trace.Tracer;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polling transactional outbox relay using PostgreSQL {@code FOR UPDATE SKIP LOCKED}.
 * Guarantees at-least-once event publication consistent with committed order state,
 * while downstream idempotent consumers deduplicate by {@code eventId}.
 */
@Component
public class OutboxRelayPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Tracer tracer;
    private final int batchSize;

    public OutboxRelayPublisher(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            Tracer tracer,
            @Value("${order.outbox.batch-size:50}") int batchSize) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${order.outbox.poll-interval-ms:150}")
    @Transactional
    public int relayPendingBatch() {
        List<OutboxEventRecord> pending = outboxRepository.claimUnpublishedBatch(batchSize);
        int publishedCount = 0;
        for (OutboxEventRecord row : pending) {
            try {
                SpecificRecord avroPayload = AvroJsonCodec.fromJson(row.eventType(), row.payloadJson());
                ProducerRecord<String, Object> record = KafkaTracePropagator.tracedProducerRecord(
                        tracer,
                        row.topic(),
                        row.aggregateId(),
                        avroPayload,
                        row.traceparent()
                );
                kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
                outboxRepository.markPublished(row.id(), Instant.now());
                publishedCount++;
            } catch (Exception ex) {
                log.warn("Outbox publish failed for id={} orderId={} topic={}: {}",
                        row.id(), row.aggregateId(), row.topic(), ex.getMessage());
                outboxRepository.recordFailure(row.id(), ex.getMessage());
            }
        }
        return publishedCount;
    }
}
