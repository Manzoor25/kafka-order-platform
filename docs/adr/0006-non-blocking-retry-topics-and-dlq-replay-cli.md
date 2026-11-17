# ADR-0006: Non-Blocking Exponential Retry Topics, Dead-Letter Topic, and Replay CLI

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
In `shipping-service`, a single malformed payload (`DeserializationException`) or transient carrier API outage on one order must not stall healthy orders behind it on the same partition (head-of-line blocking). Conversely, quarantined dead-letter messages require operational tooling to inspect failure causes and replay safely after remediation.

## Decision
1. Configure `shipping-service` with Spring Kafka **non-blocking retry topics** mapped onto explicit Topics-as-Code (`doNotAutoCreateRetryTopics()`):
   - `inventory.reserved.v1.retry-0` (1,000 ms backoff)
   - `inventory.reserved.v1.retry-1` (2,000 ms backoff)
   - `inventory.reserved.v1.retry-2` (4,000 ms backoff)
   - `inventory.reserved.v1.dlt` (terminal dead-letter topic)
2. Route `DeserializationException` and `SerializationException` **directly to `.dlt`** with zero futile retry hops, using `ErrorHandlingDeserializer` on the consumer and a hybrid `AvroOrRawBytesSerializer` on the producer so raw corrupt `byte[]` payloads preserve exact forensic bytes in `.dlt`.
3. Build a standalone **`dlq-cli`** module supporting:
   - `inspect`: decodes both valid Apicurio Avro records and raw corrupt byte payloads up to `endOffsets`, displaying DLT exception headers and `traceparent`.
   - `replay`: strips stale `kafka_dlt-*` and `retry_topic-*` headers (preventing retry-loop metadata pollution), preserves `traceparent`, stamps `x-dlq-replayed-at`, optionally overrides remediated fields (`--override-postal-code`), and republishes to the source topic keyed by `orderId`.

## Consequences
- **Positive:** Partition throughput remains unblocked during poison-pill incidents; operators gain auditable one-command inspection and replay.
- **Negative:** Moving a failing order onto `.retry-*` topics temporarily relaxes strict ordering *for that specific failing order* relative to newer events for the same `orderId`—acceptable here because an order awaiting shipment has no concurrent downstream events until shipment succeeds or fails.
