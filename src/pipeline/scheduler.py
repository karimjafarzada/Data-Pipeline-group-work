"""Run incremental loads once a day while the scheduler service is running."""

import logging
import os
import time

from pipeline.__main__ import main

logger = logging.getLogger(__name__)


def schedule(interval: int) -> None:
    if interval < 1:
        raise ValueError("SCHEDULE_INTERVAL_SECONDS must be positive")
    while True:
        started_at = time.monotonic()
        logger.info("Starting scheduled incremental run")
        exit_code = main(["run", "--mode", "incremental"])
        if exit_code:
            logger.warning("Scheduled run exited with code %d", exit_code)
        # Keep starts a day apart and avoid overlapping a run that takes longer.
        time.sleep(max(1, interval - (time.monotonic() - started_at)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    schedule(int(os.environ.get("SCHEDULE_INTERVAL_SECONDS", "86400")))
