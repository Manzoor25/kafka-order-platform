CREATE TABLE customer_notifications (
    notification_id   VARCHAR(64) PRIMARY KEY,
    dedup_key         VARCHAR(128) NOT NULL UNIQUE,
    order_id          VARCHAR(64)  NOT NULL,
    recipient_email   VARCHAR(255) NOT NULL,
    notification_type VARCHAR(64)  NOT NULL,
    subject           VARCHAR(255) NOT NULL,
    body              TEXT         NOT NULL,
    trace_id          VARCHAR(64),
    sent_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customer_notifications_order ON customer_notifications (order_id);

CREATE TABLE processed_events (
    event_id       VARCHAR(128) PRIMARY KEY,
    event_type     VARCHAR(128) NOT NULL,
    order_id       VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
