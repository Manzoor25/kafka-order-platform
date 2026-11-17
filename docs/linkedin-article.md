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

- **Durability quorum:** All **15 topics** (11 domain topics, 3 exponential retry topics, and 1 dead-letter topic) run across a **3-broker Kafka 4.4.0 KRaft cluster** with **6 partitions**, **replication factor 3**, and **`min.insync.replicas=2`**, keyed by `orderId`. Pairing `acks=all` with `unclean.leader.election.enable=false` ensures every acknowledged write sits on at least 2 in-sync brokers, while `6 = lcm(1, 2, 3, 6)` partitions rebalance evenly across 1, 2, 3, or 6 consumer instances under **KIP-848 (`group.protocol=consumer`)**.
- **Transactional Outbox (`FOR UPDATE SKIP LOCKED`):** `order-service` writes `orders`, `order_saga_history`, and serialized Avro bytes (plus the active W3C `traceparent` header) into `outbox_events` inside one PostgreSQL ACID transaction. A polling relay claims batches with `SKIP LOCKED` every 150 ms and publishes with `acks=all`.
- **Choreographed Saga with Fan-Out Compensation:** Forward flow runs `OrderCreated -> PaymentAuthorized -> InventoryReserved -> ShipmentCreated -> OrderConfirmed`. When `shipping-service` rejects an unserviceable destination (`ShipmentFailed`), `inventory-service` emits `InventoryReleased` to restore stock while `payment-service` refunds the card and emits `PaymentRefunded`, driving `order-service` to `CANCELLED`.
- **Where Kafka EOS Stops:** `payment-service` uses Spring's `KafkaTransactionManager` so consuming `OrderCreated` and producing `PaymentAuthorized` commit atomically in `__transaction_state`. Crucially, Kafka's coordinator cannot roll back an external card gateway call. We close that gap by passing a deterministic idempotency key (`"charge:" + orderId` and `"refund:" + orderId`) across the gateway boundary, paired with `processed_events(event_id)` tables in every PostgreSQL consumer.
- **Non-Blocking Retries, DLT & CLI Replay:** `shipping-service` routes transient failures across `.retry-0` (1 s), `.retry-1` (2 s), and `.retry-2` (4 s) into `inventory.reserved.v1.dlt`, while deserialization failures jump straight to `.dlt` via a hybrid `AvroOrRawBytesSerializer` that preserves raw corrupt forensic bytes. A standalone `dlq-cli` inspects mixed Avro/raw DLT messages, strips stale `kafka_dlt-*` headers, preserves `traceparent`, and replays remediated records.

---

## The One Thing That Surprised Me

What surprised me wasn't the Kafka transaction API—it was how silently **distributed tracing and dead-letter handling break at infrastructure seams** until you test them end to end.

First, enabling OpenTelemetry auto-instrumentation on HTTP and `KafkaTemplate` still split every order into two disconnected traces in Jaeger. Why? Because `POST /api/orders` finishes and clears its thread-local span before the outbox relay thread wakes up 150 ms later. Persisting W3C `traceparent` inside `outbox_events` and re-parenting the relay span stitched all **5 microservices** back into a single trace tree—even across DLT replay hours later.

Second, configuring Apicurio's Avro serializer globally caused `DeadLetterPublishingRecoverer` to fail whenever a truly corrupt non-Avro poison pill hit the consumer: the recoverer tried to Avro-serialize raw `byte[]` garbage on its way to `.dlt`! Writing a small delegating `AvroOrRawBytesSerializer` let typed `SpecificRecord` retries keep schema validation while passing corrupt `byte[]` payloads straight into `.dlt` untouched so the partition kept moving.

---

## Measured Results (and What Was Not Measured)

Every invariant is verified automatically in GitHub Actions across **9 Maven modules**:

- **Fast build & contract suite:** **23 unit, schema-contract, and zero-HTTP architecture tests** compile and pass in **~25 seconds**, validating **11 baseline Avro schemas** and rejecting breaking schema evolutions with non-zero exit code `1` (`READER_FIELD_MISSING_DEFAULT_VALUE`).
- **Containerized integration & 5-service E2E suite:** **8 Testcontainers integration classes** boot real `apache/kafka:4.4.0`, `apicurio/apicurio-registry:3.3.4`, and 5 isolated `postgres:17.11-alpine` schemas to verify happy path, 5-service W3C trace continuity, and all 3 saga compensation paths.
- **Broker `SIGKILL` under load:** Across **30 live orders** submitted while sending `SIGKILL` to `kafka-2` at order #10, **30 / 30 acknowledged orders (100%)** completed with **0 lost messages** and **0 under-replicated partitions** once `kafka-2` rejoined.
- **Payment `SIGKILL` mid-transaction:** Killing `payment-service` with `SIGKILL` after the external card charge executed—while the uncommitted `PaymentAuthorized` sat inside an open Kafka transaction—resulted in **`executionCount = 1`** card charge and **`deduplicatedRetryCount = 1`** on recovery.
- **KIP-848 scaling:** Scaling `inventory-service` from **1 to 3 instances** reassigned the **6 partitions** cleanly from `6` on 1 member to **`2, 2, 2`** across 3 members with **6 / 6 concurrent orders** completing uninterrupted.

**What was not measured:** All tests ran on single-host Docker Compose and CI runners rather than multi-AZ bare metal. I did **not** benchmark maximum sustained throughput saturation (orders/sec ceiling), multi-hour tail latencies (p99/p99.9 under disk contention), Debezium vs. SQL-polling CPU crossover points above 5,000 writes/sec, or asymmetric network partitions (packet loss / clock skew via `tc netem`).

---

## Explore the Code

If you want to inspect the ADRs, run `make up && make demo`, or throw `SIGKILL` at brokers and Kafka transactions yourself, the full source, scripts, and failure demos are here:

**Repository:** [https://github.com/Manzoor25/kafka-order-platform](https://github.com/Manzoor25/kafka-order-platform)
