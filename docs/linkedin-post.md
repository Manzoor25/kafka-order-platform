Turning on Kafka transactions does NOT stop your payment service from charging a customer's credit card twice.

Kafka's transaction coordinator guarantees atomic commits across input offsets and output topics—and stops cold at the broker boundary. The moment your consumer calls Stripe, writes to Postgres, or sends an email, `exactly_once_v2` becomes a boundary problem, not a checkbox.

To test every failure seam hands-on, I built a runnable 5-service order platform on Apache Kafka 4.4.0 (KRaft), Java 21, Spring Boot 4.1.2, PostgreSQL, Apicurio (Avro), and OpenTelemetry/Jaeger—with zero HTTP calls between services:

- SQL transactional outbox (`FOR UPDATE SKIP LOCKED`) + W3C `traceparent` stitching 1 trace across all 5 services
- Choreographed saga with full compensation (stock release + card refund)
- Kafka EOS in `payment-service` + gateway idempotency keys: `SIGKILL` mid-transaction -> 1 card charge, 1 deduplicated retry
- `SIGKILL` of a live broker under load: 30/30 acknowledged orders completed, 0 lost
- Non-blocking retry topics, `.dlt` + `dlq-cli` replay, KIP-848 1->3 scaling, and CI `BACKWARD` schema gates

Repo + ADRs + failure demos: https://github.com/Manzoor25/kafka-order-platform

#ApacheKafka #DistributedSystems #Java #SpringBoot
