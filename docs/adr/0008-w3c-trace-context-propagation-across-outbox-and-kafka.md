# ADR-0008: End-to-End W3C Trace Context Propagation Across Transactional Outbox and Kafka

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
In a pure event-driven choreography with zero HTTP calls between services, debugging latency or compensation paths across five services is opaque unless every asynchronous boundary preserves a single distributed `traceId`. Standard Kafka observation hooks break across transactional outbox tables because the background polling thread (`OutboxRelayScheduler`) runs outside the originating HTTP request thread's `ThreadLocal` trace context.

## Decision
1. Enable **Micrometer Tracing + OpenTelemetry OTLP export** (`spring-boot-starter-opentelemetry` -> Jaeger 2.23.0 OTLP HTTP `:4318/v1/traces`) with `setObservationEnabled(true)` on both `KafkaTemplate` and `ConcurrentKafkaListenerContainerFactory`.
2. Explicitly bridge the transactional outbox thread boundary via `KafkaTraceHeaderSupport`:
   - During `POST /api/orders` (and saga state transitions), `OrderSagaService` captures the active W3C `traceparent` (`00-<traceId>-<spanId>-01`) and persists it in `outbox_events.traceparent` (and `orders.trace_id`).
   - When `OutboxRelayScheduler` publishes the `ProducerRecord`, it attaches `traceparent` to the Kafka record headers and wraps the send in a child span parented to that stored `traceparent`.
   - Downstream consumers (`payment`, `inventory`, `shipping`, `notification`) automatically extract W3C `traceparent` from Kafka headers, emit child spans under the identical `traceId`, and propagate `traceparent` onto every produced and replayed DLT record.

## Consequences
- **Positive:** Every order yields a single unified 5-service trace tree in Jaeger (`order-service` -> `payment-service` -> `inventory-service` -> `shipping-service` -> `order-service` + `notification-service`), verified automatically in both `PlatformSagaEndToEndIT` and `make demo`.
- **Negative:** Adds 55 bytes (`traceparent` header) per Kafka message and requires an explicit column on `outbox_events`.
