from __future__ import annotations

from datetime import datetime

from pymongo.collection import Collection
from pymongo.database import Database

from replicator.repository import EPOCH


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
