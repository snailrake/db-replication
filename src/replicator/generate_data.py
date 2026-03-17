from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg

from replicator.config import Settings


FIRST_NAMES = [
    "Ivan",
    "Maria",
    "Alex",
    "Olga",
    "Dmitry",
    "Anna",
    "Nikita",
    "Elena",
]

LAST_NAMES = [
    "Petrov",
    "Sidorova",
    "Smirnov",
    "Ivanova",
    "Volkov",
    "Orlova",
    "Kuznetsov",
    "Morozova",
]

PRODUCTS = [
    "Laptop",
    "Mouse",
    "Keyboard",
    "Monitor",
    "Desk Lamp",
    "Headphones",
    "USB Hub",
    "Webcam",
]

STATUSES = ["pending", "completed", "shipped", "cancelled"]


def configure_logging(log_level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger("seed-generator")


def customer_row(customer_id: int) -> tuple[str, str, datetime, datetime]:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    email = f"customer_{customer_id}@example.com"
    created_at = datetime.utcnow() - timedelta(days=random.randint(1, 90))
    updated_at = created_at + timedelta(hours=random.randint(0, 48))
    return name, email, created_at, updated_at


def order_row(customer_id: int) -> tuple[int, str, Decimal, str, datetime, datetime]:
    created_at = datetime.utcnow() - timedelta(days=random.randint(0, 90))
    updated_at = created_at + timedelta(hours=random.randint(0, 72))
    amount = Decimal(random.randint(500, 250000)) / Decimal("1.00")
    return (
        customer_id,
        f"Order for {random.choice(PRODUCTS)}",
        amount,
        random.choice(STATUSES),
        created_at,
        updated_at,
    )


def product_row(product_id: int) -> tuple[str, str, Decimal, datetime, datetime]:
    name = f"{random.choice(PRODUCTS)} {product_id}"
    sku = f"SKU-{product_id:06d}"
    created_at = datetime.utcnow() - timedelta(days=random.randint(1, 120))
    updated_at = created_at + timedelta(hours=random.randint(0, 72))
    price = Decimal(random.randint(500, 250000)) / Decimal("1.00")
    return name, sku, price, created_at, updated_at


def insert_customers(
    conn: psycopg.Connection, start_id: int, count: int, batch_size: int, logger: logging.Logger
) -> None:
    inserted = 0
    with conn.cursor() as cur:
        while inserted < count:
            batch_count = min(batch_size, count - inserted)
            rows = [customer_row(start_id + inserted + offset) for offset in range(batch_count)]
            cur.executemany(
                """
                INSERT INTO customers (name, email, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            inserted += batch_count
            logger.info("Inserted customers: %s/%s", inserted, count)


def insert_products(
    conn: psycopg.Connection, start_id: int, count: int, batch_size: int, logger: logging.Logger
) -> None:
    inserted = 0
    with conn.cursor() as cur:
        while inserted < count:
            batch_count = min(batch_size, count - inserted)
            rows = [product_row(start_id + inserted + offset) for offset in range(batch_count)]
            cur.executemany(
                """
                INSERT INTO products (name, sku, price, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            inserted += batch_count
            logger.info("Inserted products: %s/%s", inserted, count)


def insert_orders(
    conn: psycopg.Connection,
    customer_ids: list[int],
    count: int,
    batch_size: int,
    logger: logging.Logger,
) -> list[int]:
    inserted = 0
    last_order_id = fetch_last_order_id(conn)
    new_order_ids: list[int] = []
    with conn.cursor() as cur:
        while inserted < count:
            batch_count = min(batch_size, count - inserted)
            rows = [order_row(random.choice(customer_ids)) for _ in range(batch_count)]
            cur.executemany(
                """
                INSERT INTO orders (customer_id, product, amount, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            inserted += batch_count
            logger.info("Inserted orders: %s/%s", inserted, count)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM orders WHERE id > %s ORDER BY id", (last_order_id,))
        new_order_ids = [row[0] for row in cur.fetchall()]
    return new_order_ids


def insert_order_products(
    conn: psycopg.Connection,
    order_ids: list[int],
    product_ids: list[int],
    max_products_per_order: int,
    batch_size: int,
    logger: logging.Logger,
) -> None:
    inserted = 0
    total_orders = len(order_ids)
    with conn.cursor() as cur:
        for start in range(0, total_orders, batch_size):
            batch_order_ids = order_ids[start : start + batch_size]
            batch_rows: list[tuple[int, int, datetime, datetime]] = []
            for order_id in batch_order_ids:
                product_count = random.randint(1, max_products_per_order)
                selected_products = random.sample(product_ids, k=min(product_count, len(product_ids)))
                for product_id in selected_products:
                    created_at = datetime.utcnow() - timedelta(days=random.randint(0, 90))
                    updated_at = created_at + timedelta(hours=random.randint(0, 24))
                    batch_rows.append((order_id, product_id, created_at, updated_at))
            cur.executemany(
                """
                INSERT INTO order_products (order_id, product_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id, product_id) DO NOTHING
                """,
                batch_rows,
            )
            conn.commit()
            inserted += len(batch_order_ids)
            logger.info("Processed order-product links for orders: %s/%s", inserted, total_orders)


def apply_soft_delete(
    conn: psycopg.Connection,
    table_name: str,
    id_column: str,
    ratio: float,
    logger: logging.Logger,
) -> None:
    if ratio <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table_name}
            SET deleted_at = NOW() - (random() * INTERVAL '24 hours')
            WHERE {id_column} IN (
                SELECT {id_column}
                FROM {table_name}
                WHERE deleted_at IS NULL
                ORDER BY random()
                LIMIT (
                    SELECT FLOOR(COUNT(*) * %s)::INT
                    FROM {table_name}
                    WHERE deleted_at IS NULL
                )
            )
            """,
            (ratio,),
        )
        affected = cur.rowcount
        conn.commit()
    logger.info("Marked %s rows as soft-deleted in %s", affected, table_name)


def fetch_existing_customer_ids(conn: psycopg.Connection) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM customers ORDER BY id")
        return [row[0] for row in cur.fetchall()]


def fetch_existing_product_ids(conn: psycopg.Connection) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM products ORDER BY id")
        return [row[0] for row in cur.fetchall()]


def fetch_last_customer_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM customers")
        return cur.fetchone()[0]


def fetch_last_product_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM products")
        return cur.fetchone()[0]


def fetch_last_order_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM orders")
        return cur.fetchone()[0]


def main() -> None:
    settings = Settings()
    logger = configure_logging(settings.log_level)
    random.seed(42)

    logger.info("Connecting to PostgreSQL")
    with psycopg.connect(settings.postgres_dsn) as conn:
        customer_start_id = fetch_last_customer_id(conn) + 1
        logger.info("Generating %s customers", settings.seed_customers)
        insert_customers(
            conn,
            customer_start_id,
            settings.seed_customers,
            settings.seed_batch_size,
            logger,
        )

        product_start_id = fetch_last_product_id(conn) + 1
        logger.info("Generating %s products", settings.seed_products)
        insert_products(
            conn,
            product_start_id,
            settings.seed_products,
            settings.seed_batch_size,
            logger,
        )

        customer_ids = fetch_existing_customer_ids(conn)
        product_ids = fetch_existing_product_ids(conn)
        logger.info("Generating %s orders", settings.seed_orders)
        order_ids = insert_orders(conn, customer_ids, settings.seed_orders, settings.seed_batch_size, logger)

        logger.info("Generating order-product links")
        insert_order_products(
            conn,
            order_ids,
            product_ids,
            settings.seed_max_products_per_order,
            settings.seed_batch_size,
            logger,
        )

        apply_soft_delete(conn, "customers", "id", settings.seed_soft_delete_ratio, logger)
        apply_soft_delete(conn, "orders", "id", settings.seed_soft_delete_ratio, logger)
        apply_soft_delete(conn, "products", "id", settings.seed_soft_delete_ratio, logger)

    logger.info("Data generation completed")


if __name__ == "__main__":
    main()
