"""Prometheus metrics of the archiver and the HTTP server that exposes them on ``/metrics``."""

import logging
import math
import shutil
from pathlib import Path

from aiohttp import web
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    disable_created_metrics,
    generate_latest,
)

logger = logging.getLogger("archiver.metrics")

disable_created_metrics()

DEFAULT_PORT = 9108

ALERTS_WRITTEN = Counter("archiver_alerts_written", "Alerts appended to the archive")
LAST_ALERT = Gauge(
    "archiver_last_alert_timestamp_seconds",
    "Unix time at which the last alert was written",
)
INVALID_ALERTS = Counter("archiver_invalid_alerts", "Alerts skipped because they are not valid JSON or have no timestamp")
WRITE_ERRORS = Counter("archiver_write_errors", "Alerts lost because the disk write failed")

STREAM_CONNECTED = Gauge("archiver_stream_connected", "1 while the Vigil stream is open, else 0")
STREAM_DISCONNECTS = Counter(
    "archiver_stream_disconnects",
    "Stream losses, by reason",
    ["reason"],
)
DISCONNECT_REASONS = ("closed", "timeout", "http_error", "connection_error")
for _reason in DISCONNECT_REASONS:
    STREAM_DISCONNECTS.labels(_reason)

MAINTENANCE_RUNS = Counter("archiver_maintenance_runs", "Maintenance runs, by result", ["result"])
for _result in ("success", "failure"):
    MAINTENANCE_RUNS.labels(_result)
MAINTENANCE_LAST_SUCCESS = Gauge(
    "archiver_maintenance_last_success_timestamp_seconds",
    "Unix time of the last maintenance run that finished without error",
)
ZIP_FAILURES = Counter("archiver_zip_failures", "Day folders that could not be zipped")

ARCHIVE_BYTES = Gauge("archiver_archive_bytes", "Total size of the archive directory")
DISK_FREE_BYTES = Gauge("archiver_disk_free_bytes", "Free space on the file system of the archive directory")


def directory_size(directory: Path) -> float:
    """Return the size in bytes of every file under ``directory``, or NaN if it cannot be read."""
    try:
        return float(sum(p.stat().st_size for p in directory.rglob("*") if p.is_file()))
    except OSError:
        return math.nan


def disk_free(directory: Path) -> float:
    """Return the free bytes of the file system holding ``directory``, or NaN if it cannot be read."""
    try:
        return float(shutil.disk_usage(directory).free)
    except OSError:
        return math.nan


def watch_archive(directory: str | Path) -> None:
    """Compute the archive size and free disk space at each scrape."""
    path = Path(directory)
    ARCHIVE_BYTES.set_function(lambda: directory_size(path))
    DISK_FREE_BYTES.set_function(lambda: disk_free(path))


def build_app(registry: CollectorRegistry = REGISTRY) -> web.Application:
    async def metrics(_request: web.Request) -> web.Response:
        response = web.Response(body=generate_latest(registry))
        response.headers["Content-Type"] = CONTENT_TYPE_LATEST
        return response

    app = web.Application()
    app.add_routes([web.get("/metrics", metrics)])
    return app


async def start_server(host: str, port: int) -> web.AppRunner:
    """Serve ``/metrics`` on ``host:port``; the caller must call ``cleanup()`` on the returned runner."""
    runner = web.AppRunner(build_app(), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    logger.info("Metrics served on http://%s:%d/metrics", host, port)
    return runner
