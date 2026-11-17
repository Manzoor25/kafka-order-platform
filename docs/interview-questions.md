# 10 Architecture & Design Interview Questions (with Grounded Answers)

### 1. Why isn't enabling Kafka's `exactly_once_v2` (`KafkaTransactionManager` + `read_committed`) enough to prevent charging a customer's card twice?
**Answer:** Kafka's transaction coordinator (`__transaction_state`) only controls atomic visibility across **Kafka partitions and `__consumer_offsets`**. It has no two-phase commit hook into Stripe, Adyen, or PostgreSQL. If `payment-service` authorizes the card over HTTP, flushes `PaymentAuthorized` into an open Kafka transaction, and suffers a `SIGKILL` before `commitTransaction()`, Kafka aborts the uncommitted `PaymentAuthorized` record and leaves the `OrderCreated` offset uncommitted. When the consumer restarts, Kafka **must** redeliver `OrderCreated`. Only passing a deterministic idempotency key (`"charge:" + orderId`) to the payment gateway prevents that redelivery from executing a second card capture (`scripts/demo-kill-payment-mid-tx.sh` proves `executionCount = 1, deduplicatedRetryCount = 1`).

---

### 2. What happens in `inventory-service` if the JVM crashes *after* committing the PostgreSQL stock decrement, *before* publishing `InventoryReserved` or committing the Kafka offset?
**Answer:** Spring's `ChainedKafkaTransactionManager` was deprecated specifically because committing JDBC first and Kafka second still leaves a narrow crash window between the two commits. Instead, `inventory-service` embraces **crash-safe idempotent replay**:
1. Inside one PostgreSQL transaction, it decrements `sku_stock`, inserts `stock_reservations(order_id UNIQUE, status = RESERVED)`, and inserts `processed_events(event_id)` **before** committing the Kafka offset (`AckMode.MANUAL_IMMEDIATE`).
2. If the JVM dies before publishing `InventoryReserved` or committing the Kafka offset, Kafka redelivers `PaymentAuthorized`.
3. On redelivery, `InventoryReservationService` detects that `processed_events` already contains `event_id`, **skips** decrementing `sku_stock` a second time, reloads the persisted `ReservationRow` for `orderId`, **re-publishes `InventoryReserved`**, and acknowledges the offset. Stock is decremented once, and downstream saga progress is never stranded.

---

### 3. Why did you choose a polling transactional outbox (`FOR UPDATE SKIP LOCKED`) in `order-service` over Debezium CDC? When would that break down?
**Answer:** A `FOR UPDATE SKIP LOCKED` relay inside `order-service` requires zero additional infrastructure (no Kafka Connect cluster, no connector offset/config/status topics, and no risk of an orphaned PostgreSQL logical replication slot filling disk with retained WAL segments during a Connect outage). Moreover, because `order-service` serializes the Apicurio Avro payload and W3C `traceparent` directly into `outbox_events`, the relay forwards exact wire bytes with zero SMT transformation complexity.
**When it breaks down:** Above ~5,000–10,000 writes/sec or across dozens of outbox tables, continuous polling creates measurable index churn and adds ~75–150 ms latency. At that scale, tailing PostgreSQL WAL sequentially via Debezium Outbox Event Router becomes cheaper on database CPU and achieves sub-20 ms p50 relay latency.

---

### 4. Does moving a failing message to non-blocking retry topics (`.retry-0`, `.retry-1`, `.retry-2`) violate per-`orderId` ordering?
**Answer:** In general, yes—non-blocking retry topics trade strict per-key ordering on the failing key in exchange for keeping the main partition unblocked for all other keys. In our order saga specifically, **stage-gated causality makes this safe**: `shipping-service` consumes `inventory.reserved.v1` only after payment and stock reservation have completed, and no subsequent lifecycle event (`OrderConfirmed` or `ShipmentFailed` compensation) can exist for that `orderId` until `shipping-service` emits an outcome. If a domain allowed concurrent mutable updates on the same entity (e.g., `ShippingAddressUpdated` racing an in-retry `InventoryReserved`), we would maintain a "retrying keys" tombstone/state marker in `shipping-db` so subsequent events for an already-retrying `orderId` are routed into the same retry queue to preserve per-key FIFO order.

---

### 5. What if `ShipmentFailed` triggers both `inventory-service` (`InventoryReleased`) and `payment-service` (`PaymentRefunded`), and one of those compensating consumers is temporarily down?
**Answer:** Because `shipping.failed.v1` is replicated across 3 brokers (`RF=3, min.insync.replicas=2`) and each service uses an independent consumer group (`inventory-service-group` and `payment-service-group`) with manual offset commits, both compensations are independently durable. If `inventory-service` is down for 10 minutes, `payment-service` immediately refunds the customer and `order-service` marks the order `CANCELLED`; as soon as `inventory-service` recovers, it resumes from its last committed offset on `shipping.failed.v1` and releases the reserved SKU quantity idempotently. Neither service needs the other to be online simultaneously.

---

### 6. Why `partitions = 6`, `replication.factor = 3`, and `min.insync.replicas = 2`? Why not `min.insync.replicas = 3` for maximum safety?
**Answer:** Setting `min.insync.replicas = 3` when `replication.factor = 3` converts a 3-broker cluster from fault-tolerant HA into a brittle **zero-fault-tolerance** write path: a routine rolling restart or single broker crash immediately halts every `acks=all` producer across the entire shop with `NotEnoughReplicasException`. With `RF = 3`, `min.insync.replicas = 2`, `acks = all`, and `unclean.leader.election.enable = false`, every acknowledged record sits on at least **2** brokers (`W = 2`), and only an in-sync broker can become leader (`W + R > 3`). Thus we achieve **identical zero-data-loss guarantees** against single-broker failure while preserving 100% write availability during rolling upgrades or unplanned node crashes. `6` partitions (`lcm(1, 2, 3, 6)`) guarantees balanced assignment across 1, 2, 3, or 6 consumer instances.

---

### 7. What concretely improves when switching from `group.protocol=classic` to KIP-848 (`group.protocol=consumer`) in Kafka 4.x?
**Answer:** Under `classic`, adding or removing even one `inventory-service` pod forces every member of `inventory-service-group` through synchronized `JoinGroup` and `SyncGroup` barriers, electing one client JVM to deserialize all subscriptions and compute partition assignments while revoking partitions synchronously. Under **KIP-848 (`group.protocol=consumer`)**:
1. Partition assignment moves **onto the broker group coordinator** (`uniform` assignor), eliminating client leader CPU spikes and split-brain view mismatches.
2. Members heartbeat asynchronously and converge toward target epochs incrementally: when scaling from `1 -> 3` instances (`scripts/demo-scale-kip848.sh`), instance 1 keeps processing its remaining `2` partitions uninterrupted while smoothly yielding only the `4` migrated partitions (`2` each to instances 2 and 3).

---

### 8. Why enforce `BACKWARD` compatibility instead of `FORWARD`, and how would you remove or rename an Avro field in production?
**Answer:** `BACKWARD` compatibility guarantees that a **new consumer** compiled against schema `v2` can deserialize historical and in-flight messages written with schema `v1`—including replaying hours or days of Kafka backlog or `.dlt` records. That creates a deterministic deployment order: **upgrade consumers first, then upgrade producers**.
To rename or remove a required field safely without breaking `BACKWARD` compatibility:
1. **Additive phase (`v2`):** Add the new field as optional with a default (`["null", "string"], "default": null`) and dual-populate both fields in producers.
2. **Migration phase:** Migrate all consumers to read the new field (falling back to the legacy field for old records).
3. **Major cutover (only if physical removal is required):** Either retain the deprecated field defaulted in `.v1` indefinitely (minimal byte cost in binary Avro) or introduce `order.created.v2` once retention expires.

---

### 9. How did you keep a single OpenTelemetry trace unbroken across the PostgreSQL transactional outbox and DLQ replay?
**Answer:** Automatic Spring Kafka instrumentation relies on an active `Observation` in thread-local context at `kafkaTemplate.send(...)` time. A transactional outbox breaks that assumption because `POST /api/orders` finishes and clears its HTTP thread before `OutboxRelayScheduler` wakes up 150 ms later on a scheduled worker thread.
We solved this explicitly:
1. `OrderSagaService` serializes the active W3C `traceparent` (`00-<traceId>-<spanId>-01`) into `outbox_events.traceparent` inside the HTTP request's DB transaction.
2. `OutboxRelayScheduler` reconstructs parent trace context from `outbox_events.traceparent`, starts a child span (`outbox.publish`), and writes `traceparent` onto the outbound Kafka `ProducerRecord` headers.
3. Similarly, `dlq-cli replay` strips `kafka_dlt-*` / `retry_topic-*` failure headers so Spring Retry treats the message cleanly, while explicitly **preserving `traceparent`** so the replayed completion joins the original order's Jaeger trace.

---

### 10. What happens when traffic grows 20x and you need to expand `order.created.v1` from 6 partitions to 24 partitions?
**Answer:** Increasing partitions on an existing keyed topic changes `murmur2(orderId) mod numPartitions` from `mod 6` to `mod 24`. For **brand-new `orderId`s**, events immediately co-locate on the new partition consistently. However, for **in-flight orders** created seconds before expansion, an earlier event sitting unconsumed in partition `P_old` and a new event landing in `P_new` could be processed concurrently out of order.
Two safe production migration patterns avoid in-flight reordering:
1. **Dual-topic cutover (`order.created.v2` with 24 partitions):** Deploy consumers subscribed to both `.v1` and `.v2`, switch `order-service` outbox writes to `.v2`, drain `.v1` lag to zero, and retire `.v1`.
2. **Brief ingress drain window:** Pause outbox relay for ~2 seconds until consumer lag on `.v1` hits `0`, alter partitions `6 -> 24`, wait for metadata refresh, and resume relay—safe because all prior per-order events on that hop have already been consumed.
