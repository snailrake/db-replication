INSERT INTO customers (name, email, created_at, updated_at, deleted_at)
VALUES
    ('Ivan Petrov', 'ivan@example.com', NOW() - INTERVAL '3 day', NOW() - INTERVAL '3 day', NULL),
    ('Maria Sidorova', 'maria@example.com', NOW() - INTERVAL '2 day', NOW() - INTERVAL '2 day', NULL),
    ('Alex Smirnov', 'alex@example.com', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day', NULL);

INSERT INTO products (name, sku, price, created_at, updated_at, deleted_at)
VALUES
    ('Laptop', 'SKU-LAPTOP-001', 75000.00, NOW() - INTERVAL '4 day', NOW() - INTERVAL '3 day', NULL),
    ('Mouse', 'SKU-MOUSE-001', 1500.00, NOW() - INTERVAL '4 day', NOW() - INTERVAL '3 day', NULL),
    ('Monitor', 'SKU-MONITOR-001', 35000.00, NOW() - INTERVAL '4 day', NOW() - INTERVAL '2 day', NULL),
    ('Keyboard', 'SKU-KEYBOARD-001', 4200.00, NOW() - INTERVAL '4 day', NOW() - INTERVAL '1 day', NOW() - INTERVAL '10 hour'),
    ('USB Hub', 'SKU-HUB-001', 2200.00, NOW() - INTERVAL '2 day', NOW() - INTERVAL '1 day', NULL);

INSERT INTO orders (customer_id, product, amount, status, created_at, updated_at, deleted_at)
VALUES
    (1, 'Office setup', 76500.00, 'completed', NOW() - INTERVAL '3 day', NOW() - INTERVAL '2 day', NULL),
    (1, 'Accessories order', 3700.00, 'pending', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day', NULL),
    (2, 'Display upgrade', 35000.00, 'shipped', NOW() - INTERVAL '2 day', NOW() - INTERVAL '1 day', NULL),
    (3, 'Input devices', 4200.00, 'completed', NOW() - INTERVAL '12 hour', NOW() - INTERVAL '12 hour', NOW() - INTERVAL '3 hour');

INSERT INTO order_products (order_id, product_id, created_at, updated_at, deleted_at)
VALUES
    (1, 1, NOW() - INTERVAL '3 day', NOW() - INTERVAL '2 day', NULL),
    (1, 2, NOW() - INTERVAL '3 day', NOW() - INTERVAL '2 day', NULL),
    (2, 2, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day', NULL),
    (2, 5, NOW() - INTERVAL '1 day', NOW() - INTERVAL '8 hour', NOW() - INTERVAL '2 hour'),
    (3, 3, NOW() - INTERVAL '2 day', NOW() - INTERVAL '1 day', NULL),
    (4, 4, NOW() - INTERVAL '12 hour', NOW() - INTERVAL '12 hour', NULL);
