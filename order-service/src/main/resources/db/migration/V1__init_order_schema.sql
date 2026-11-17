CREATE TABLE orders (
    order_id             VARCHAR(64)  PRIMARY KEY,
    customer_id          VARCHAR(64)  NOT NULL,
    customer_email       VARCHAR(255) NOT NULL,
    sku                  VARCHAR(64)  NOT NULL,
    quantity             INT          NOT NULL CHECK (quantity > 0),
    total_amount_cents   BIGINT       NOT NULL CHECK (total_amount_cents > 0),
    currency             VARCHAR(8)   NOT NULL,
    payment_method_token VARCHAR(128) NOT NULL,
    shipping_postal_code VARCHAR(32)  NOT NULL,
    coupon_code          VARCHAR(64),
    status               VARCHAR(32)  NOT NULL,
    payment_id           VARCHAR(64),
    reservation_id       VARCHAR(64),
    shipment_id          VARCHAR(64),
    tracking_number      VARCHAR(64),
    failure_stage        VARCHAR(64),
    failure_reason       VARCHAR(512),
    trace_id             VARCHAR(64),
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE order_saga_history (
    id            BIGSERIAL    PRIMARY KEY,
    order_id      VARCHAR(64)  NOT NULL REFERENCES orders (order_id),
    from_status   VARCHAR(32),
    to_status     VARCHAR(32)  NOT NULL,
    trigger_event VARCHAR(64)  NOT NULL,
    detail        VARCHAR(512),
    occurred_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_order_saga_history_order ON order_saga_history (order_id, id);

CREATE TABLE outbox_events (
    id            UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id  VARCHAR(64)  NOT NULL,
    topic         VARCHAR(128) NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload_json  TEXT         NOT NULL,
    traceparent   VARCHAR(128),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ,
    attempt_count INT          NOT NULL DEFAULT 0,
    last_error    VARCHAR(512)
);

CREATE INDEX idx_outbox_unpublished
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id       VARCHAR(128) PRIMARY KEY,
    event_type     VARCHAR(128) NOT NULL,
    order_id       VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
