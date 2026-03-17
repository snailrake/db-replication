from __future__ import annotations

from replicator.config import Settings
from replicator.replication import ReplicationWorker, configure_logging, verify_replication


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        verify_replication(settings)
        return

    worker = ReplicationWorker(settings)
    if settings.worker_mode == "once":
        worker.run_once()
        return

    worker.run_forever()


if __name__ == "__main__":
    main()
