from __future__ import annotations

import logging
import time
from datetime import datetime

from pymongo import ReplaceOne

from replicator.clients import mongo_database, postgres_connection
from replicator.config import Settings
from replicator.repository import fetch_customer_snapshot, fetch_impacted_customer_ids
from replicator.scheduler import seconds_until_next_run
from replicator.state import SyncStateStore
from replicator.transform import build_customer_documents


def chunked(items: list[int], size: int) -> list[list[int]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


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
