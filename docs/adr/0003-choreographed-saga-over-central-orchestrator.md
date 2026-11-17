# ADR-0003: Choreographed Saga over Central Command Orchestrator

- **Status:** Accepted
- **Date:** 2026-04-16

## Context
Fulfilling an order spans five bounded contexts (`order`, `payment`, `inventory`, `shipping`, `notification`) with strict autonomy: no service may call another over HTTP. Failures at payment, stock reservation, or carrier booking must unwind all prior side effects deterministically.

## Decision
Implement a **choreographed event-driven saga** using domain facts rather than point-to-point RPC commands:

- **Forward path:** `OrderCreated` -> `PaymentAuthorized` -> `InventoryReserved` -> `ShipmentCreated` -> `OrderConfirmed`.
- **Compensation path 1 (Payment Declined):** `PaymentDeclined` -> `OrderCancelled`.
- **Compensation path 2 (Out of Stock):** `InventoryReservationFailed` -> `PaymentRefunded` -> `OrderCancelled`.
- **Compensation path 3 (Shipping Rejected):** `ShipmentFailed` -> `InventoryReleased` (in `inventory-service`) + `PaymentRefunded` (in `payment-service`) -> `OrderCancelled` (in `order-service`).

Each service owns its local state transitions and listens only to upstream domain facts. `order-service` subscribes to milestone and terminal events (`PaymentAuthorized`, `PaymentDeclined`, `PaymentRefunded`, `InventoryReserved`, `ShipmentCreated`) to maintain an auditable `order_saga_history` read model without issuing imperative commands to downstream services.

## Consequences
- **Positive:** High service autonomy; `notification-service` or future fraud/analytics consumers subscribe to existing fact topics with zero changes to upstream producers. No central orchestrator SPOF.
- **Negative:** End-to-end workflow topology is distributed across listener classes rather than a single orchestrator DSL; mitigated by OpenTelemetry W3C distributed tracing (`traceId` spanning all 5 services) and `order_saga_history`. If the workflow grew beyond 7–8 conditional branches, an event-sourced orchestrator would become preferable.
