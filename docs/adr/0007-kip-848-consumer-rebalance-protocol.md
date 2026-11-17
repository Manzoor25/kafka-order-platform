# ADR-0007: KIP-848 Next-Generation Consumer Group Protocol (`group.protocol=consumer`)

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
Classic Kafka consumer groups (`group.protocol=classic`) rely on client-driven `JoinGroup` / `SyncGroup` rebalance barriers where a coordinator elects a leader consumer to compute partition assignments, causing stop-the-world pauses or multi-round cooperative revocations during horizontal scaling. Apache Kafka 4.x makes the KIP-848 next-generation consumer group coordinator production-ready.

## Decision
Enable **`KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer`** on all three Kafka 4.4.0 KRaft brokers and configure microservices with **`group.protocol=consumer`** (and server-side `uniform` assignment) in `KafkaSerdeFactory`.

## Consequences
- **Positive:** Scaling `inventory-service` from `1` to `3` instances (`demo-scale-kip848.sh`) shifts partition computation to the broker group coordinator asynchronously; only the `4` migrated partitions (`6 -> 2, 2, 2`) are incrementally revoked and reassigned while the remaining `2` partitions keep processing continuously without global `SyncGroup` barriers.
- **Negative:** Client-side `ConsumerPartitionAssignor` plugins are superseded by broker-side assignors (`uniform`, `range`); configurations setting legacy `partition.assignment.strategy` alongside `group.protocol=consumer` are rejected by Kafka 4.x clients, so `KafkaSerdeFactory` explicitly omits legacy client assignor properties when `consumer` protocol is active.
