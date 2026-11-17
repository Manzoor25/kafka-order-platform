# ADR-0005: Kafka Transactions (EOS) Boundary in Payment Service and Idempotent Side Effects

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
`payment-service` performs a classic read-process-write transformation (`OrderCreated` -> `PaymentAuthorized` / `PaymentDeclined`, and compensation -> `PaymentRefunded`), while also calling an external card processor (`SimulatedCardGateway`). Engineers frequently assume enabling Kafka transactions (`transactional.id` + `isolation.level=read_committed`) automatically makes the entire payment flow "exactly-once."

## Decision
1. Configure **`payment-service`** with Spring Kafka's **`KafkaTransactionManager`** (`eosKafkaListenerContainerFactory`, `transactional.id.prefix=payment-eos-tx-...`) and configure **all consumers platform-wide** with `isolation.level=read_committed` and manual offset commits (`AckMode.MANUAL_IMMEDIATE`).
2. Explicitly treat Kafka's transactional coordinator (`__transaction_state`) as an **internal log-to-log boundary only**: it atomically commits the consumed `OrderCreated` offset alongside the produced `PaymentAuthorized` event, and hides uncommitted/aborted records from downstream `read_committed` consumers.
3. Protect external side effects **outside** Kafka's transaction coordinator using **deterministic idempotency keys**:
   - External card gateway calls pass `"charge:" + orderId` and `"refund:" + orderId` (`SimulatedCardGateway` commits in `Propagation.REQUIRES_NEW` to model an external HTTP card network). If `payment-service` crashes after charging the card but before committing the Kafka transaction, redelivery hits the same idempotency key and returns the prior authorization without double-charging.
   - All PostgreSQL state mutations in every service record `event_id` in a local `processed_events` table (`IdempotentEventGuard`) inside the service's local JDBC transaction before committing Kafka offsets.

## Consequences
- **Positive:** Achieves true end-to-end effectively-once behavior across both Kafka topics and external side effects without distributed 2PC/XA locks. Verified under hard `SIGKILL` mid-transaction (`demo-kill-payment-mid-tx.sh`).
- **Negative:** Transactional producers incur slight commit marker overhead on `__transaction_state` and require `transaction.state.log.replication.factor=3` and `transaction.state.log.min.isr=2`.
