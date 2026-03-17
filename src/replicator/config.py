from __future__ import annotations

import os
from dataclasses import dataclass


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None else default


@dataclass(frozen=True)
class Settings:
    postgres_db: str = _get_env("POSTGRES_DB", "shop")
    postgres_user: str = _get_env("POSTGRES_USER", "admin")
    postgres_password: str = _get_env("POSTGRES_PASSWORD", "secret")
    postgres_host: str = _get_env("POSTGRES_HOST", "postgres")
    postgres_port: int = int(_get_env("POSTGRES_PORT", "5432"))

    mongo_db: str = _get_env("MONGO_INITDB_DATABASE", "replica")
    mongo_host: str = _get_env("MONGO_HOST", "mongo")
    mongo_port: int = int(_get_env("MONGO_PORT", "27017"))

    log_level: str = _get_env("LOG_LEVEL", "INFO")
    worker_mode: str = _get_env("WORKER_MODE", "cron")
    sync_on_start: bool = _get_env("SYNC_ON_START", "true").lower() == "true"
    batch_size: int = int(_get_env("BATCH_SIZE", "1000"))
    sync_interval_seconds: int = int(_get_env("SYNC_INTERVAL_SECONDS", "60"))
    sync_cron: str = _get_env("SYNC_CRON", "*/2 * * * *")
    sync_job_id: str = _get_env("SYNC_JOB_ID", "postgres_to_mongo")

    state_backend: str = _get_env("STATE_BACKEND", "mongo")
    state_collection: str = _get_env("STATE_COLLECTION", "sync_state")
    customers_collection: str = _get_env("CUSTOMERS_COLLECTION", "customers")
    seed_customers: int = int(_get_env("SEED_CUSTOMERS", "100000"))
    seed_products: int = int(_get_env("SEED_PRODUCTS", "500"))
    seed_orders: int = int(_get_env("SEED_ORDERS", "500000"))
    seed_batch_size: int = int(_get_env("SEED_BATCH_SIZE", "5000"))
    seed_max_products_per_order: int = int(_get_env("SEED_MAX_PRODUCTS_PER_ORDER", "3"))
    seed_soft_delete_ratio: float = float(_get_env("SEED_SOFT_DELETE_RATIO", "0.01"))

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def mongo_dsn(self) -> str:
        return f"mongodb://{self.mongo_host}:{self.mongo_port}/"
