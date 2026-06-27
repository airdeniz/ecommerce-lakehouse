CREATE TABLE users (
    user_id     BIGSERIAL PRIMARY KEY,
    full_name   TEXT        NOT NULL,
    city        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id  BIGINT PRIMARY KEY,
    name        TEXT           NOT NULL,
    category    TEXT           NOT NULL,
    price       NUMERIC(10,2)  NOT NULL
);

CREATE TABLE inventory (
    product_id  BIGINT PRIMARY KEY REFERENCES products(product_id),
    stock_qty   INT    NOT NULL
);

CREATE TABLE orders (
    order_id     BIGSERIAL PRIMARY KEY,
    user_id      BIGINT        NOT NULL REFERENCES users(user_id),
    status       TEXT          NOT NULL DEFAULT 'CREATED',
    total_amount NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES orders(order_id),
    product_id    BIGINT NOT NULL REFERENCES products(product_id),
    quantity      INT    NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL
);

ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE inventory   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE users       REPLICA IDENTITY FULL;