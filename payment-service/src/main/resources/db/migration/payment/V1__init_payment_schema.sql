-- Simulates the external card processor's idempotency ledger (where Kafka EOS stops!).
CREATE TABLE gateway_operations (
    idempotency_key          VARCHAR(128) PRIMARY KEY,
    order_id                 VARCHAR(64)  NOT NULL,
    operation_type           VARCHAR(32)  NOT NULL,
    gateway_reference_id     VARCHAR(64)  NOT NULL,
    amount_cents             BIGINT       NOT NULL,
    currency                 VARCHAR(8)   NOT NULL,
    outcome                  VARCHAR(32)  NOT NULL,
    reason                   VARCHAR(255),
    execution_count          INT          NOT NULL DEFAULT 1,
    deduplicated_retry_count INT          NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_gateway_operations_order ON gateway_operations (order_id);

CREATE TABLE payments (
    payment_id        VARCHAR(64) PRIMARY KEY,
    order_id          VARCHAR(64) NOT NULL UNIQUE,
    customer_email    VARCHAR(255) NOT NULL,
    amount_cents      BIGINT       NOT NULL,
    currency          VARCHAR(8)   NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    gateway_charge_id VARCHAR(64),
    gateway_refund_id VARCHAR(64),
    failure_reason    VARCHAR(255),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id       VARCHAR(128) PRIMARY KEY,
    event_type     VARCHAR(128) NOT NULL,
    order_id       VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
