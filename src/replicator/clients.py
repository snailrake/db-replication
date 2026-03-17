from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from pymongo import MongoClient
from pymongo.database import Database

from replicator.config import Settings


@contextmanager
def postgres_connection(settings: Settings) -> Iterator[psycopg.Connection]:
    with psycopg.connect(settings.postgres_dsn) as conn:
        yield conn


@contextmanager
def mongo_client(settings: Settings) -> Iterator[MongoClient]:
    client = MongoClient(settings.mongo_dsn)
    try:
        yield client
    finally:
        client.close()


@contextmanager
def mongo_database(settings: Settings) -> Iterator[Database]:
    with mongo_client(settings) as client:
        yield client[settings.mongo_db]
