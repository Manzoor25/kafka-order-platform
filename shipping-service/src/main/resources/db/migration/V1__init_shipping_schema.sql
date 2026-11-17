CREATE TABLE shipments (
    shipment_id          VARCHAR(64) PRIMARY KEY,
    order_id             VARCHAR(64)  NOT NULL UNIQUE,
    reservation_id       VARCHAR(64)  NOT NULL,
    sku                  VARCHAR(64)  NOT NULL,
    quantity             INT          NOT NULL,
    carrier              VARCHAR(32),
    tracking_number      VARCHAR(64),
    shipping_postal_code VARCHAR(32)  NOT NULL,
    status               VARCHAR(32)  NOT NULL,
    failure_reason       VARCHAR(255),
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE dead_letter_audit (
    id                BIGSERIAL PRIMARY KEY,
    dlt_topic         VARCHAR(128) NOT NULL,
    dlt_partition     INT          NOT NULL,
    dlt_offset        BIGINT       NOT NULL,
    record_key        VARCHAR(128),
    payload_type      VARCHAR(128) NOT NULL,
    exception_message TEXT,
    observed_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id       VARCHAR(128) PRIMARY KEY,
    event_type     VARCHAR(128) NOT NULL,
    order_id       VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
