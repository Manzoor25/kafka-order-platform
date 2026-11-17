# Exactly-once is a boundary, not a feature: what building an order saga on Kafka taught me

"Just turn on Kafka transactions—that gives us exactly-once payments, right?"

I have heard some version of that sentence in almost every architecture review involving money movement and event streams. On paper, `enable.idempotence=true`, `transactional.id`, and `isolation.level=read_committed` sound like a silver bullet. To test where that guarantee actually holds—and exactly where it stops—I built a complete, open-source five-service order platform on **Apache Kafka 4.4.0 (KRaft)** using **Java 21**, **Spring Boot 4.1.2**, **PostgreSQL 17.11**, **Apicurio Registry 3.3.4 (Avro 1.12.2)**, and **OpenTelemetry / Jaeger 2.23.0**, then tried to break every hop with fault injection.

---

## The Problem

An online shop accepts an order, charges the customer's card, reserves stock, books a carrier shipment, and sends customer notifications. Five services (`order-service`, `payment-service`, `inventory-service`, `shipping-service`, and `notification-service`) each own a dedicated PostgreSQL database and communicate **exclusively through Kafka events**—zero inter-service HTTP calls, enforced on every build by a source-scanning architecture test.

Without synchronous RPC, four failure modes immediately dominate the design:

1. **Dual-write split brain:** `order-service` cannot commit an order row in PostgreSQL and fail to publish `OrderCreated` (or publish an event for a rolled-back transaction).
2. **Crash mid-payment:** `payment-service` cannot charge a credit card twice if the JVM dies after the card network approves the charge but before Kafka commits the consumer offset.
3. **Multi-hop rollbacks:** If stock runs out after payment succeeds—or carrier booking fails after both payment and stock succeed—prior side effects must unwind deterministically without a central orchestrator calling REST rollback endpoints.
4. **Poison messages:** One corrupt byte payload or flaky postal-code lookup cannot block an entire partition behind it.

---

## The Design

Every architectural choice went into a short ADR alongside explicit Topics-as-Code (`auto.create.topics.enable=false`):

- **Durability quorum:** All **55 topics** (11 domain streams, each provisioned with 3 exponential retry topics and 1 dead-letter topic) run across a **3-broker Kafka 4.4.0 KRaft cluster** with **6 partitions**, **replication factor 3**, and **`min.insync.replicas=2`**, keyed by `orderId`. Pairing `acks=all` with `unclean.leader.election.enable=false` ensures every acknowledged write sits on at least 2 in-sync brokers, while `6 = lcm(1, 2, 3, 6)` partitions rebalance evenly across 1, 2, 3, or 6 consumer instances under **KIP-848 (`group.protocol=consumer`)**.
- **Transactional Outbox (`FOR UPDATE SKIP LOCKED`):** `order-service` writes `orders`, `order_saga_history`, and serialized Avro JSON (plus the active W3C `traceparent` header) into `outbox_events` inside one PostgreSQL ACID transaction. A polling relay claims batches with `SKIP LOCKED` every 150 ms and publishes with `acks=all`.
- **Choreographed Saga with Fan-Out Compensation:** Forward flow runs `OrderCreated -> PaymentAuthorized -> InventoryReserved -> ShipmentCreated -> OrderConfirmed`. When `shipping-service` rejects an unserviceable destination (`ShipmentFailed`), `inventory-service` emits `InventoryReleased` to restore stock while `payment-service` refunds the card and emits `PaymentRefunded`, driving `order-service` to `CANCELLED`.
- **Where Kafka EOS Stops:** `payment-service` uses Spring's `KafkaTransactionManager` so consuming `OrderCreated` and producing `PaymentAuthorized` commit atomically in `__transaction_state`. Crucially, Kafka's coordinator cannot roll back an external card gateway call. We close that gap by passing a deterministic idempotency key (`"charge:" + orderId` and `"refund:" + orderId`) across the gateway boundary, paired with `processed_events(event_id)` tables in every PostgreSQL consumer.
- **Non-Blocking Retries, DLT & CLI Replay:** `shipping-service` routes transient failures across `.retry-0` (1 s), `.retry-1` (2 s), and `.retry-2` (4 s) into `inventory.reserved.v1.dlt`, while deserialization failures jump straight to `.dlt` via paired `AvroOrRawBytesSerializer` and `AvroOrRawBytesDeserializer` adapters that preserve raw corrupt forensic bytes without double-faulting DLT listeners. A standalone `dlq-cli` inspects mixed Avro/raw DLT messages, strips stale `kafka_dlt-*` headers, preserves `traceparent`, and replays remediated records.

---

## The One Thing That Surprised Me

What surprised me wasn't the Kafka transaction API—it was how silently **distributed tracing and dead-letter handling break at infrastructure seams** until you test them end to end.

First, enabling OpenTelemetry auto-instrumentation on HTTP and `KafkaTemplate` still split every order into disconnected traces in Jaeger. Not only does `POST /api/orders` finish before the outbox relay wakes up 150 ms later, Spring Boot 4's `@Scheduled` instrumentation starts its own poll-loop span that overwrites `KafkaTemplate`'s outgoing `traceparent` unless the restored outbox `Context` stays active across `kafkaTemplate.send()`. Keeping that scope open across the broker send stitched all **5 microservices** back into a single trace tree—even across DLT replay.

Second, configuring Apicurio's Avro serde globally broke dead-letter handling on both ends whenever raw non-Avro bytes hit a topic: `DeadLetterPublishingRecoverer` tried to Avro-serialize raw `byte[]` garbage on the way into `.dlt`, and the `.dlt` audit consumer threw `DeserializationException` all over again reading it back out. Delegating primary and retry topics strictly to Avro while passing raw `byte[]` through on `.dlt` kept schema enforcement intact without wedging a single partition.

---

## Measured Results (and What Was Not Measured)

Every invariant runs automatically in GitHub Actions across **9 Maven modules** and **15 Docker Compose containers**:

- **Automated Maven verification (`4 min 12 s` in CI):** **38 tests across 18 test classes** (**28** fast unit, schema-contract, and zero-HTTP architecture tests + **10** Testcontainers integration and 5-service E2E saga tests) pass with **0 failures**, validating **11 baseline Avro schemas** and rejecting breaking evolutions with exit code `1` (`READER_FIELD_MISSING_DEFAULT_VALUE` / Apicurio HTTP `409`).
- **Live 3-broker KRaft Compose & fault-injection suite (`5 min 36 s` in CI):** Boots all **15 containers** (`apache/kafka:4.4.0` × 3, `postgres:17.11-alpine` × 5, `apicurio/apicurio-registry:3.3.4`, `jaegertracing/jaeger:2.23.0`, `kafbat/kafka-ui:v1.5.0`, plus all 5 services) and verifies Jaeger correlates all **5 services** onto a single 32-hex `traceId`.
- **Broker `SIGKILL` under load:** Across **30 live orders** submitted while sending `SIGKILL` to `kafka-2` at order #10, **30 / 30 acknowledged orders (100%)** completed with **0 lost messages** and **0 under-replicated partitions** once `kafka-2` rejoined.
- **Payment `SIGKILL` mid-transaction:** Killing `payment-service` with `SIGKILL` after the external card charge executed—while the uncommitted `PaymentAuthorized` sat inside an open Kafka transaction—resulted in **`executionCount = 1`** card charge and **`deduplicatedRetryCount = 1`** on recovery.
- **KIP-848 scaling:** Scaling `inventory-service` from **1 to 3 instances** reassigned the **6 partitions** cleanly from `0..5` on 1 member to **`2, 2, 2`** (`{0,1}`, `{2,5}`, `{3,4}`) across 3 members with **6 / 6 concurrent orders** completing uninterrupted.

**What was not measured:** All tests ran on single-host Docker Compose and CI runners rather than multi-AZ bare metal. I did **not** benchmark maximum sustained throughput saturation (orders/sec ceiling), multi-hour tail latencies (p99/p99.9 under disk contention), Debezium vs. SQL-polling CPU crossover points above 5,000 writes/sec, or asymmetric network partitions (packet loss / clock skew via `tc netem`).

---

## Explore the Code

If you want to inspect the ADRs, run `make up && make demo`, or throw `SIGKILL` at brokers and Kafka transactions yourself, the full source, scripts, and failure demos are here:

**Repository:** [https://github.com/Manzoor25/kafka-order-platform](https://github.com/Manzoor25/kafka-order-platform)
