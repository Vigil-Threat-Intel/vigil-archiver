import asyncio
import json
import math
import time
from datetime import datetime, timezone

import aiohttp
import pytest
from aiohttp import web
from prometheus_client import REGISTRY

from src import metrics
from src.client import StreamClient
from src.store import AlertStore
from tests.test_client import serve


def value(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_write_counts_alert_and_sets_last_alert_time(tmp_path):
    before = value("archiver_alerts_written_total")
    start = time.time()
    store = AlertStore(tmp_path)
    asyncio.run(store.write(json.dumps({"timestamp": "2026-11-24T14:00:00+00:00"})))
    assert value("archiver_alerts_written_total") == before + 1
    assert value("archiver_last_alert_timestamp_seconds") >= start


def test_rejected_alert_is_not_counted_as_written(tmp_path):
    before = value("archiver_alerts_written_total")
    with pytest.raises(ValueError):
        asyncio.run(AlertStore(tmp_path).write("not json"))
    assert value("archiver_alerts_written_total") == before


def test_maintenance_success_and_zip_failure(tmp_path):
    (tmp_path / "2026-11-23.zip").write_bytes(b"garbage")
    (tmp_path / "2026-11-23").mkdir()
    (tmp_path / "2026-11-23" / "10.jsonl").write_text("x\n")
    clock = lambda: datetime(2026, 11, 24, 12, tzinfo=timezone.utc)
    runs, zip_failures = value("archiver_maintenance_runs_total", result="success"), value("archiver_zip_failures_total")
    start = time.time()

    async def go():
        task = asyncio.create_task(AlertStore(tmp_path, clock=clock).run_maintenance(interval=10))
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
    assert value("archiver_maintenance_runs_total", result="success") == runs + 1
    assert value("archiver_maintenance_last_success_timestamp_seconds") >= start
    assert value("archiver_zip_failures_total") == zip_failures + 1


@pytest.mark.parametrize(
    "exc, reason",
    [
        (asyncio.TimeoutError(), "timeout"),
        (aiohttp.ServerTimeoutError(), "timeout"),
        (aiohttp.ClientResponseError(None, (), status=503), "http_error"),
        (aiohttp.ClientConnectionError(), "connection_error"),
        (ConnectionRefusedError(), "connection_error"),
    ],
)
def test_disconnect_reason(exc, reason):
    assert StreamClient.disconnect_reason(exc) == reason


def test_stream_connected_gauge_and_closed_disconnects():
    seen_connected = []

    async def handler(request):
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"data: x\n\n")
        return response

    async def go():
        runner, url = await serve(handler)

        async def on_alert(alert):
            seen_connected.append(value("archiver_stream_connected"))

        task = asyncio.create_task(StreamClient(url, on_alert, reconnect_delay=0.05).run())
        await asyncio.sleep(0.3)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()

    before = value("archiver_stream_disconnects_total", reason="closed")
    asyncio.run(go())
    assert seen_connected and all(v == 1 for v in seen_connected)
    assert value("archiver_stream_connected") == 0
    assert value("archiver_stream_disconnects_total", reason="closed") > before


def test_connection_refused_is_counted():
    async def go():
        async def on_alert(alert):
            pass

        task = asyncio.create_task(StreamClient("http://127.0.0.1:9/stream", on_alert, reconnect_delay=0.05).run())
        await asyncio.sleep(0.3)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    before = value("archiver_stream_disconnects_total", reason="connection_error")
    asyncio.run(go())
    assert value("archiver_stream_disconnects_total", reason="connection_error") > before


def test_archive_size_and_disk_free(tmp_path):
    (tmp_path / "2026-11-24").mkdir()
    (tmp_path / "2026-11-24" / "10.jsonl").write_bytes(b"x" * 100)
    (tmp_path / "2026-11-23.zip").write_bytes(b"y" * 50)
    metrics.watch_archive(tmp_path)
    assert value("archiver_archive_bytes") == 150
    assert value("archiver_disk_free_bytes") > 0
    assert metrics.directory_size(tmp_path / "absent") == 0
    assert math.isnan(metrics.disk_free(tmp_path / "absent"))


def test_metrics_endpoint_serves_prometheus_text():
    async def go():
        runner = await metrics.start_server("127.0.0.1", 0)
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/metrics") as response:
                    return response.status, response.headers["Content-Type"], await response.text()
        finally:
            await runner.cleanup()

    status, content_type, body = asyncio.run(go())
    assert status == 200
    assert content_type.startswith("text/plain")
    for name in (
        "archiver_alerts_written_total",
        "archiver_stream_connected",
        'archiver_stream_disconnects_total{reason="timeout"}',
        'archiver_maintenance_runs_total{result="failure"}',
        "process_start_time_seconds",
    ):
        assert name in body
