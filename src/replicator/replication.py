from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator

import psycopg
from croniter import croniter
from psycopg.rows import dict_row
from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database

from replicator.config import Settings


EPOCH = datetime(1970, 1, 1)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@contextmanager
def postgres_connection(settings: Settings) -> Iterator[psycopg.Connection]:
    with psycopg.connect(settings.postgres_dsn) as conn:
        yield conn


@contextmanager
def mongo_database(settings: Settings) -> Iterator[Database]:
    client = MongoClient(settings.mongo_dsn)
    try:
        yield client[settings.mongo_db]
    finally:
        client.close()


class SyncStateStore:
    def __init__(self, database: Database, collection_name: str, job_id: str) -> None:
        self.collection: Collection = database[collection_name]
        self.job_id = job_id

    def read_last_sync(self) -> datetime:
        state = self.collection.find_one({"_id": self.job_id})
        if not state:
            return EPOCH
        return state.get("last_synced_at", EPOCH)

    def save_last_sync(self, synced_at: datetime) -> None:
        self.collection.update_one(
            {"_id": self.job_id},
            {"$set": {"last_synced_at": synced_at, "updated_at": datetime.utcnow()}},
            upsert=True,
        )


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


def _as_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def build_customer_documents(rows: list[dict[str, Any]], synced_at: datetime) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    customers_map: dict[int, dict[str, Any]] = {}
    orders_map: dict[tuple[int, int], dict[str, Any]] = {}

    for row in rows:
        customer_id = row["customer_id"]
        customer_doc = customers_map.get(customer_id)
        if customer_doc is None:
            customer_doc = {
                "_id": customer_id,
                "name": row["customer_name"],
                "email": row["customer_email"],
                "created_at": row["customer_created_at"],
                "updated_at": row["customer_updated_at"],
                "deleted_at": row["customer_deleted_at"],
                "synced_at": synced_at,
                "orders": [],
            }
            customers_map[customer_id] = customer_doc
            documents.append(customer_doc)

        order_id = row["order_id"]
        if order_id is None:
            continue

        order_key = (customer_id, order_id)
        order_doc = orders_map.get(order_key)
        if order_doc is None:
            order_doc = {
                "order_id": order_id,
                "product": row["order_product"],
                "amount": _as_float(row["order_amount"]),
                "status": row["order_status"],
                "placed_at": row["order_created_at"],
                "updated_at": row["order_updated_at"],
                "deleted_at": row["order_deleted_at"],
                "products": [],
            }
            orders_map[order_key] = order_doc
            customer_doc["orders"].append(order_doc)

        if row["product_id"] is None:
            continue

        product_doc = {
            "product_id": row["product_id"],
            "name": row["product_name"],
            "sku": row["product_sku"],
            "price": _as_float(row["product_price"]),
            "created_at": row["product_created_at"],
            "updated_at": row["product_updated_at"],
            "deleted_at": row["product_deleted_at"],
        }
        order_doc["products"].append(product_doc)

    return documents


def chunked(items: list[int], size: int) -> list[list[int]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def seconds_until_next_run(cron_expression: str) -> float:
    now = datetime.now()
    next_run = croniter(cron_expression, now).get_next(datetime)
    delay = (next_run - now).total_seconds()
    return max(delay, 0.0)


class ReplicationWorker:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = logging.getLogger("replicator.worker")

    def check_connections(self) -> None:
        with postgres_connection(self.settings) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

        with mongo_database(self.settings) as database:
            database.command("ping")

        self.logger.info("Connections to PostgreSQL and MongoDB are healthy")

    def _run_replication_cycle(self) -> None:
        self.logger.info("Starting replication cycle")
        upper_bound = datetime.utcnow()
        replicated_customers = 0
        replicated_orders = 0

        with postgres_connection(self.settings) as conn, mongo_database(self.settings) as database:
            state_store = SyncStateStore(
                database=database,
                collection_name=self.settings.state_collection,
                job_id=self.settings.sync_job_id,
            )
            last_sync = state_store.read_last_sync()
            impacted_customer_ids = fetch_impacted_customer_ids(conn, last_sync, upper_bound)

            if not impacted_customer_ids:
                state_store.save_last_sync(upper_bound)
                self.logger.info("No changes detected since %s", last_sync.isoformat())
                return

            customers_collection = database[self.settings.customers_collection]

            for customer_batch in chunked(impacted_customer_ids, self.settings.batch_size):
                snapshot_rows = fetch_customer_snapshot(conn, customer_batch)
                customer_documents = build_customer_documents(snapshot_rows, synced_at=upper_bound)

                operations = [
                    ReplaceOne({"_id": document["_id"]}, document, upsert=True)
                    for document in customer_documents
                ]
                if operations:
                    customers_collection.bulk_write(operations, ordered=False)

                replicated_customers += len(customer_documents)
                replicated_orders += sum(len(document["orders"]) for document in customer_documents)

            state_store.save_last_sync(upper_bound)

        self.logger.info(
            "Replicated %s customers and %s orders up to %s",
            replicated_customers,
            replicated_orders,
            upper_bound.isoformat(),
        )

    def run_once(self) -> None:
        self.check_connections()
        self._run_replication_cycle()

    def run_forever(self) -> None:
        if self.settings.sync_on_start:
            self._run_safe_cycle()

        if self.settings.worker_mode == "interval":
            while True:
                self.logger.info(
                    "Sleeping for %s seconds before the next replication cycle",
                    self.settings.sync_interval_seconds,
                )
                time.sleep(self.settings.sync_interval_seconds)
                self._run_safe_cycle()

        while True:
            delay = seconds_until_next_run(self.settings.sync_cron)
            self.logger.info(
                "Next replication cycle is scheduled in %.2f seconds using cron '%s'",
                delay,
                self.settings.sync_cron,
            )
            time.sleep(delay)
            self._run_safe_cycle()

    def _run_safe_cycle(self) -> None:
        try:
            self.run_once()
        except Exception:
            self.logger.exception("Replication cycle failed")


def verify_replication(settings: Settings) -> None:
    with postgres_connection(settings) as conn, mongo_database(settings) as database:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            customers_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM orders")
            orders_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM products")
            products_count = cur.fetchone()[0]

        mongo_customers = database[settings.customers_collection]
        replicated_customers = mongo_customers.count_documents({})
        sample_document = mongo_customers.find_one(sort=[("_id", 1)])
        state_document = database[settings.state_collection].find_one({"_id": settings.sync_job_id})

    print("PostgreSQL counts:")
    print(f"  customers: {customers_count}")
    print(f"  orders: {orders_count}")
    print(f"  products: {products_count}")
    print()
    print("MongoDB counts:")
    print(f"  customer documents: {replicated_customers}")
    print()
    print("Sync state:")
    print(state_document)
    print()
    print("Sample replicated document:")
    print(sample_document)
