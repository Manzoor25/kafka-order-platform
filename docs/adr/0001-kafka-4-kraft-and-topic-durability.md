# ADR-0001: Apache Kafka 4.4.0 KRaft Cluster and Topic Durability Topology

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
The order platform routes money movement, inventory reservation, and shipping state exclusively over Kafka events. Any lost acknowledged event leaves an order stranded mid-saga; any out-of-order delivery for the same order can apply compensating transitions before forward ones. Kafka 4.x removes ZooKeeper completely in favor of KRaft consensus.

## Decision
1. Run a **3-node Apache Kafka 4.4.0 KRaft cluster** (`broker,controller` combined roles for local Docker Compose; separable roles in production) with `auto.create.topics.enable=false`.
2. Provision all 55 topics explicitly as code (`PlatformTopicsConfiguration`: 11 domain streams × `[1 primary + 3 retry + 1 dlt]`) with:
   - **`partitions = 6`** (`lcm(1, 2, 3, 6)`), allowing uniform partition assignment across 1, 2, 3, or 6 consumer instances without idle threads or excessive open segment files.
   - **`replication.factor = 3`**, ensuring every partition exists on all 3 brokers.
   - **`min.insync.replicas = 2`** paired with producer `acks = all`, `enable.idempotence = true`, and broker `unclean.leader.election.enable = false`.
3. Key every domain, retry, and dead-letter record by **`orderId`** so all lifecycle events for a single order hash deterministically to the same partition number.

## Consequences
- **Positive:** Surviving `2` of `3` brokers (`|ISR| >= 2`) satisfies quorum intersection (`W + R > N`), guaranteeing zero lost acknowledged messages when any single broker is killed under load.
- **Negative:** Losing **2 brokers simultaneously** pauses producer writes (`NotEnoughReplicasException`) rather than accepting under-replicated writes—an intentional CP safety choice over AP lossiness.
