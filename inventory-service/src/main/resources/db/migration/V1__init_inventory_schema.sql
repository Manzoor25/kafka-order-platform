CREATE TABLE stock_catalog (
    sku           VARCHAR(64) PRIMARY KEY,
    product_name  VARCHAR(255) NOT NULL,
    available_qty INT          NOT NULL CHECK (available_qty >= 0),
    reserved_qty  INT          NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE stock_reservations (
    reservation_id     VARCHAR(64) PRIMARY KEY,
    order_id           VARCHAR(64)  NOT NULL UNIQUE,
    sku                VARCHAR(64)  NOT NULL,
    quantity           INT          NOT NULL,
    available_snapshot INT          NOT NULL DEFAULT 0,
    status             VARCHAR(32)  NOT NULL,
    reason             VARCHAR(255),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id       VARCHAR(128) PRIMARY KEY,
    event_type     VARCHAR(128) NOT NULL,
    order_id       VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO stock_catalog (sku, product_name, available_qty, reserved_qty) VALUES
    ('SKU-LAPTOP-PRO',   'Workstation Laptop 16"',    50000, 0),
    ('SKU-MECH-KB',      'Hot-Swap Mechanical Keyboard', 50000, 0),
    ('SKU-HEADSET',      'Studio Wireless Headset',   50000, 0),
    ('SKU-MOUSE',        'Ergonomic Optical Mouse',   50000, 0),
    ('SKU-MONITOR-4K',   '27" 4K IPS Display',        50000, 0),
    ('SKU-LIMITED-1',    'Limited Collector Edition', 1,     0),
    ('SKU-OUT-OF-STOCK', 'Discontinued Adapter',      0,     0);
