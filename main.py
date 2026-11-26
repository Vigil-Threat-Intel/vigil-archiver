"""Vigil archiver entry point: reads the Vigil alert stream and archives it on disk."""

import argparse
import asyncio
import logging
import os

from src import metrics
from src.client import DEFAULT_URL, StreamClient
from src.store import AlertStore

logger = logging.getLogger("archiver")


async def stats_reporter(store: AlertStore, interval: float) -> None:
    """Log how many alerts were written during each interval."""
    last = store.written
    while True:
        await asyncio.sleep(interval)
        logger.info("alerts written in the last %ds: %d", interval, store.written - last)
        last = store.written


async def run(args: argparse.Namespace) -> None:
    store = AlertStore(args.archive_dir, retention_days=args.retention_days)

    async def handle(alert: str) -> None:
        try:
            await store.write(alert)
        except ValueError as exc:
            metrics.INVALID_ALERTS.inc()
            logger.warning("Invalid alert skipped (%s): %.200s", exc, alert)
        except OSError:
            metrics.WRITE_ERRORS.inc()
            logger.exception("Write failed")

    runner = None
    if args.metrics_port:
        metrics.watch_archive(args.archive_dir)
        runner = await metrics.start_server("0.0.0.0", args.metrics_port)

    client = StreamClient(args.url, handle)
    tasks = [
        asyncio.create_task(client.run(), name="stream"),
        asyncio.create_task(store.run_maintenance(), name="maintenance"),
        asyncio.create_task(stats_reporter(store, 60.0), name="stats"),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if runner is not None:
            await runner.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vigil archiver - stores the Vigil alert stream on disk")
    parser.add_argument(
        "--url",
        default=os.environ.get("VIGIL_STREAM_URL", DEFAULT_URL),
        help="Vigil stream URL (env VIGIL_STREAM_URL)",
    )
    parser.add_argument(
        "--archive-dir",
        default=os.environ.get("ARCHIVE_DIR", "/data/archive"),
        help="archive root directory (env ARCHIVE_DIR)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=int(os.environ.get("RETENTION_DAYS", "30")),
        help="days of zipped archives to keep (env RETENTION_DAYS)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.environ.get("METRICS_PORT", str(metrics.DEFAULT_PORT))),
        help="port of the Prometheus /metrics endpoint, 0 to disable (env METRICS_PORT)",
    )
    parser.add_argument("--verbose", action="store_true", help="enable DEBUG logs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("Shutdown requested")


if __name__ == "__main__":
    main()
