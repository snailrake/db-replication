from __future__ import annotations

from replicator.config import Settings
from replicator.logging_utils import configure_logging
from replicator.worker import ReplicationWorker


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    worker = ReplicationWorker(settings)
    if settings.worker_mode == "once":
        worker.run_once()
        return

    worker.run_forever()


if __name__ == "__main__":
    main()
