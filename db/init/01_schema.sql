CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS customers (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    sku         VARCHAR(100) NOT NULL UNIQUE,
    price       NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    product      VARCHAR(200) NOT NULL,
    amount       NUMERIC(10, 2) NOT NULL CHECK (amount >= 0),
    status       VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS order_products (
    order_id     BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id   BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMP NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_customers_created_at
    ON customers(created_at);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at
    ON customers(updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_created_at
    ON orders(created_at);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at
    ON orders(updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_deleted_at
    ON orders(deleted_at);

CREATE INDEX IF NOT EXISTS idx_products_created_at
    ON products(created_at);

CREATE INDEX IF NOT EXISTS idx_products_updated_at
    ON products(updated_at);

CREATE INDEX IF NOT EXISTS idx_products_deleted_at
    ON products(deleted_at);

CREATE INDEX IF NOT EXISTS idx_order_products_order_id
    ON order_products(order_id);

CREATE INDEX IF NOT EXISTS idx_order_products_product_id
    ON order_products(product_id);

CREATE INDEX IF NOT EXISTS idx_order_products_updated_at
    ON order_products(updated_at);

CREATE INDEX IF NOT EXISTS idx_order_products_deleted_at
    ON order_products(deleted_at);

DROP TRIGGER IF EXISTS trg_customers_set_updated_at ON customers;
CREATE TRIGGER trg_customers_set_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_products_set_updated_at ON products;
CREATE TRIGGER trg_products_set_updated_at
BEFORE UPDATE ON products
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_orders_set_updated_at ON orders;
CREATE TRIGGER trg_orders_set_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_order_products_set_updated_at ON order_products;
CREATE TRIGGER trg_order_products_set_updated_at
BEFORE UPDATE ON order_products
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
