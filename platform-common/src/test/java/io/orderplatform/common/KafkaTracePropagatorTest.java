package io.orderplatform.common;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.orderplatform.common.tracing.KafkaTracePropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTracePropagatorTest {

    @Test
    void preservesSingleTraceIdAcrossStoredOutboxContextAndKafkaConsumeProduceHops() {
        try (SdkTracerProvider provider = SdkTracerProvider.builder().build()) {
            OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
            Tracer tracer = otel.getTracer("test-tracer");

            String rootTraceId;
            String outboxTraceparent;
            Span ingressSpan = tracer.spanBuilder("POST /api/orders").startSpan();
            try (Scope ignored = ingressSpan.makeCurrent()) {
                rootTraceId = KafkaTracePropagator.currentTraceId();
                outboxTraceparent = KafkaTracePropagator.currentTraceparent();
            } finally {
                ingressSpan.end();
            }

            assertThat(rootTraceId).hasSize(32);
            assertThat(outboxTraceparent).startsWith("00-" + rootTraceId + "-");

            // Outbox background thread restores stored traceparent with no active thread span
            ProducerRecord<String, String> produced = KafkaTracePropagator.tracedProducerRecord(
                    tracer, "order.created.v1", "ord-42", "payload", outboxTraceparent);

            ConsumerRecord<String, String> consumed = new ConsumerRecord<>(
                    "order.created.v1", 0, 10L, "ord-42", "payload");
            produced.headers().forEach(h -> consumed.headers().add(h));

            AtomicReference<String> downstreamTraceId = new AtomicReference<>();
            AtomicReference<ProducerRecord<String, String>> nextHop = new AtomicReference<>();

            KafkaTracePropagator.executeInConsumerSpan(tracer, "payment-service process", consumed, () -> {
                downstreamTraceId.set(KafkaTracePropagator.currentTraceId());
                nextHop.set(KafkaTracePropagator.tracedProducerRecord(
                        tracer, "payment.authorized.v1", "ord-42", "paid"));
            });

            assertThat(downstreamTraceId.get()).isEqualTo(rootTraceId);
            String nextHopHeader = KafkaTracePropagator.extractTraceparentHeader(nextHop.get().headers());
            assertThat(nextHopHeader).startsWith("00-" + rootTraceId + "-");
        }
    }
}
