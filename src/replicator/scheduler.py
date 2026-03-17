from __future__ import annotations

from datetime import datetime

from croniter import croniter


def seconds_until_next_run(cron_expression: str) -> float:
    now = datetime.now()
    next_run = croniter(cron_expression, now).get_next(datetime)
    delay = (next_run - now).total_seconds()
    return max(delay, 0.0)
