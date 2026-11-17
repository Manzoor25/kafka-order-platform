package io.orderplatform.common.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Deterministic W3C TraceContext ({@code traceparent} / {@code tracestate}) propagation across
 * HTTP ingress, PostgreSQL transactional outbox rows, and Apache Kafka Producer/Consumer records.
 */
public final class KafkaTracePropagator {

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String TRACESTATE_HEADER = "tracestate";

    private static final TextMapSetter<Headers> KAFKA_HEADER_SETTER = (carrier, key, value) -> {
        if (carrier == null || key == null || value == null) {
            return;
        }
        carrier.remove(key);
        carrier.add(key, value.getBytes(StandardCharsets.UTF_8));
    };

    private static final TextMapGetter<Headers> KAFKA_HEADER_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers carrier) {
            java.util.List<String> keys = new java.util.ArrayList<>();
            for (Header h : carrier) {
                keys.add(h.key());
            }
            return keys;
        }

        @Override
        public String get(Headers carrier, String key) {
            if (carrier == null || key == null) {
                return null;
            }
            Header header = carrier.lastHeader(key);
            return header == null || header.value() == null
                    ? null
                    : new String(header.value(), StandardCharsets.UTF_8);
        }
    };

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private KafkaTracePropagator() {
    }

    public static String currentTraceparent() {
        Map<String, String> carrier = new HashMap<>();
        W3CTraceContextPropagator.getInstance().inject(Context.current(), carrier, Map::put);
        return carrier.get(TRACEPARENT_HEADER);
    }

    public static String currentTraceId() {
        SpanContext ctx = Span.current().getSpanContext();
        return ctx.isValid() ? ctx.getTraceId() : null;
    }

    public static Context extractFromTraceparent(String traceparent) {
        if (traceparent == null || traceparent.isBlank()) {
            return Context.current();
        }
        Map<String, String> carrier = Map.of(TRACEPARENT_HEADER, traceparent);
        return W3CTraceContextPropagator.getInstance().extract(Context.root(), carrier, MAP_GETTER);
    }

    public static String extractTraceparentHeader(Headers headers) {
        return KAFKA_HEADER_GETTER.get(headers, TRACEPARENT_HEADER);
    }

    public static <K, V> ProducerRecord<K, V> tracedProducerRecord(
            Tracer tracer,
            String topic,
            K key,
            V value) {
        return tracedProducerRecord(tracer, topic, key, value, null);
    }

    public static <K, V> ProducerRecord<K, V> tracedProducerRecord(
            Tracer tracer,
            String topic,
            K key,
            V value,
            String storedTraceparent) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        Context parentContext = (storedTraceparent != null && !storedTraceparent.isBlank())
                ? extractFromTraceparent(storedTraceparent)
                : Context.current();

        Span producerSpan = tracer.spanBuilder(topic + " publish")
                .setParent(parentContext)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(AttributeKey.stringKey("messaging.system"), "kafka")
                .setAttribute(AttributeKey.stringKey("messaging.destination.name"), topic)
                .setAttribute(AttributeKey.stringKey("messaging.kafka.message.key"), String.valueOf(key))
                .setAttribute(AttributeKey.stringKey("order.id"), String.valueOf(key))
                .startSpan();
        try (Scope ignored = parentContext.with(producerSpan).makeCurrent()) {
            W3CTraceContextPropagator.getInstance()
                    .inject(Context.current(), record.headers(), KAFKA_HEADER_SETTER);
        } finally {
            producerSpan.end();
        }
        return record;
    }

    public static <K, V> void executeInConsumerSpan(
            Tracer tracer,
            String operationName,
            ConsumerRecord<K, V> record,
            ThrowingRunnable action) {
        Context extracted = W3CTraceContextPropagator.getInstance()
                .extract(Context.root(), record.headers(), KAFKA_HEADER_GETTER);
        Span consumerSpan = tracer.spanBuilder(operationName)
                .setParent(extracted)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(AttributeKey.stringKey("messaging.system"), "kafka")
                .setAttribute(AttributeKey.stringKey("messaging.destination.name"), record.topic())
                .setAttribute(AttributeKey.longKey("messaging.kafka.destination.partition"), (long) record.partition())
                .setAttribute(AttributeKey.longKey("messaging.kafka.message.offset"), record.offset())
                .setAttribute(AttributeKey.stringKey("messaging.kafka.message.key"), String.valueOf(record.key()))
                .setAttribute(AttributeKey.stringKey("order.id"), String.valueOf(record.key()))
                .startSpan();

        try (Scope ignored = extracted.with(consumerSpan).makeCurrent()) {
            action.run();
            consumerSpan.setStatus(StatusCode.OK);
        } catch (RuntimeException ex) {
            consumerSpan.recordException(ex);
            consumerSpan.setStatus(StatusCode.ERROR, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            consumerSpan.recordException(ex);
            consumerSpan.setStatus(StatusCode.ERROR, ex.getMessage());
            throw new RuntimeException(ex);
        } finally {
            consumerSpan.end();
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
