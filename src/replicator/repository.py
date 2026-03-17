from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row


EPOCH = datetime(1970, 1, 1)


def fetch_impacted_customer_ids(
    conn: psycopg.Connection, last_sync: datetime, upper_bound: datetime
) -> list[int]:
    query = """
        WITH changed_customers AS (
            SELECT c.id AS customer_id
            FROM customers c
            WHERE GREATEST(c.created_at, c.updated_at, COALESCE(c.deleted_at, %(epoch)s)) > %(last_sync)s
              AND GREATEST(c.created_at, c.updated_at, COALESCE(c.deleted_at, %(epoch)s)) <= %(upper_bound)s

            UNION

            SELECT o.customer_id
            FROM orders o
            WHERE GREATEST(o.created_at, o.updated_at, COALESCE(o.deleted_at, %(epoch)s)) > %(last_sync)s
              AND GREATEST(o.created_at, o.updated_at, COALESCE(o.deleted_at, %(epoch)s)) <= %(upper_bound)s

            UNION

            SELECT o.customer_id
            FROM order_products op
            JOIN orders o ON o.id = op.order_id
            WHERE GREATEST(op.created_at, op.updated_at, COALESCE(op.deleted_at, %(epoch)s)) > %(last_sync)s
              AND GREATEST(op.created_at, op.updated_at, COALESCE(op.deleted_at, %(epoch)s)) <= %(upper_bound)s

            UNION

            SELECT o.customer_id
            FROM products p
            JOIN order_products op ON op.product_id = p.id AND op.deleted_at IS NULL
            JOIN orders o ON o.id = op.order_id
            WHERE GREATEST(p.created_at, p.updated_at, COALESCE(p.deleted_at, %(epoch)s)) > %(last_sync)s
              AND GREATEST(p.created_at, p.updated_at, COALESCE(p.deleted_at, %(epoch)s)) <= %(upper_bound)s
        )
        SELECT DISTINCT customer_id
        FROM changed_customers
        ORDER BY customer_id
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            {"epoch": EPOCH, "last_sync": last_sync, "upper_bound": upper_bound},
        )
        return [row[0] for row in cur.fetchall()]


def fetch_customer_snapshot(conn: psycopg.Connection, customer_ids: list[int]) -> list[dict[str, Any]]:
    if not customer_ids:
        return []

    query = """
        SELECT
            c.id AS customer_id,
            c.name AS customer_name,
            c.email AS customer_email,
            c.created_at AS customer_created_at,
            c.updated_at AS customer_updated_at,
            c.deleted_at AS customer_deleted_at,
            o.id AS order_id,
            o.product AS order_product,
            o.amount AS order_amount,
            o.status AS order_status,
            o.created_at AS order_created_at,
            o.updated_at AS order_updated_at,
            o.deleted_at AS order_deleted_at,
            p.id AS product_id,
            p.name AS product_name,
            p.sku AS product_sku,
            p.price AS product_price,
            p.created_at AS product_created_at,
            p.updated_at AS product_updated_at,
            p.deleted_at AS product_deleted_at
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        LEFT JOIN order_products op ON op.order_id = o.id AND op.deleted_at IS NULL
        LEFT JOIN products p ON p.id = op.product_id
        WHERE c.id = ANY(%s)
        ORDER BY c.id, o.id, p.id
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (customer_ids,))
        return list(cur.fetchall())
