from __future__ import annotations

from pprint import pprint

from replicator.clients import mongo_database, postgres_connection
from replicator.config import Settings
from replicator.logging_utils import configure_logging


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    with postgres_connection(settings) as conn, mongo_database(settings) as database:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            customers_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM orders")
            orders_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM products")
            products_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM customers WHERE deleted_at IS NOT NULL")
            deleted_customers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM orders WHERE deleted_at IS NOT NULL")
            deleted_orders = cur.fetchone()[0]

        mongo_customers = database[settings.customers_collection]
        replicated_customers = mongo_customers.count_documents({})
        sample_document = mongo_customers.find_one(sort=[("_id", 1)])
        state_document = database[settings.state_collection].find_one({"_id": settings.sync_job_id})

    print("PostgreSQL counts:")
    print(f"  customers: {customers_count}")
    print(f"  orders: {orders_count}")
    print(f"  products: {products_count}")
    print(f"  soft-deleted customers: {deleted_customers}")
    print(f"  soft-deleted orders: {deleted_orders}")
    print()
    print("MongoDB counts:")
    print(f"  customer documents: {replicated_customers}")
    print()
    print("Sync state:")
    pprint(state_document)
    print()
    print("Sample replicated document:")
    pprint(sample_document)


if __name__ == "__main__":
    main()
