# ADR-0002: Polling Transactional Outbox over Debezium CDC in Order Service

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
When `order-service` accepts `POST /api/orders` (or records a saga transition emitting `OrderConfirmed` / `OrderCancelled`), updating PostgreSQL (`orders`, `order_saga_history`) and publishing to Kafka cannot use a distributed XA transaction without tight coupling and availability bottlenecks. Dual-writing directly to PostgreSQL and `KafkaTemplate` risks divergence if the JVM crashes between the DB commit and broker acknowledgment.

Two transactional outbox relay strategies were evaluated:
1. **Transactional Polling Relay (`FOR UPDATE SKIP LOCKED`)** inside `order-service`.
2. **Log-based Change Data Capture (Debezium + Kafka Connect)** tailing PostgreSQL WAL.

## Decision
Use a **PostgreSQL transactional outbox table (`outbox_events`) with a `FOR UPDATE SKIP LOCKED` polling relay** (`OutboxRelayScheduler`, 150 ms cadence, batch size 50) inside `order-service`.

`OrderSagaService` inserts the serialized Avro payload bytes (with Apicurio's wire header), event metadata, and active W3C `traceparent` into `outbox_events` in the **same local ACID transaction** as the `orders` row mutation. Polling threads claim unpublished batches via `SELECT ... FOR UPDATE SKIP LOCKED`, publish synchronously with `acks=all`, and mark `published_at`.

## Consequences
- **Positive:** Zero extra infrastructure containers (no Kafka Connect cluster, connector offset topics, or PostgreSQL `wal_level=logical` replication slot bloat risks). Works identically across scaled `order-service` replicas without duplicate contention thanks to `SKIP LOCKED`. Preserves exact Apicurio Avro wire bytes and W3C `traceparent` headers trivially.
- **Negative:** Adds ~75–150 ms median publish latency and periodic lightweight index scans on `WHERE published_at IS NULL`. At >5,000 orders/sec sustained write throughput, migrating `outbox_events` to Debezium WAL tailing (or PostgreSQL `LISTEN/NOTIFY` wakeup + partition pruning) would reduce database polling overhead.
