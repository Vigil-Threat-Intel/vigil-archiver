# vigil-archiver

Reads the live alert stream exposed by [Vigil](../vigil_v2) (`GET /stream`, Server-Sent Events) and archives every alert on local disk. See [SPEC.md](SPEC.md) for the full specification.

## Archive layout

```
archive/
├── 2026-11-24/            current day
│   └── 14.jsonl
└── 2026-11-23.zip         finished day: one HH.jsonl per entry
```

- Files are chosen from the alert `timestamp` field, in UTC. Each line is the JSON sent by Vigil, unchanged.
- Maintenance runs at startup, then every hour: finished days are zipped, zips older than `RETENTION_DAYS` are deleted.
- Alerts emitted while the archiver is stopped or disconnected are lost.

## Configuration

| Environment variable | CLI option | Default | Description |
|---|---|---|---|
| `VIGIL_STREAM_URL` | `--url` | `http://host.docker.internal:8000/stream` | Vigil endpoint |
| `ARCHIVE_DIR` | `--archive-dir` | `/data/archive` | Archive root |
| `RETENTION_DAYS` | `--retention-days` | `30` | Days of zips kept |
| `METRICS_PORT` | `--metrics-port` | `9108` | Prometheus `/metrics` port, `0` disables it |
| none | `--verbose` | off | DEBUG logs |

The CLI option wins over the environment variable.

## Metrics

`GET /metrics` on port `9108` returns Prometheus metrics. The port is not published by `docker-compose.yml`: Prometheus must reach it on a shared Docker network.

| Metric | Type | Meaning |
|---|---|---|
| `archiver_alerts_written_total` | counter | Alerts appended to the archive |
| `archiver_last_alert_timestamp_seconds` | gauge | Unix time of the last written alert |
| `archiver_invalid_alerts_total` | counter | Alerts skipped: invalid JSON or no timestamp |
| `archiver_write_errors_total` | counter | Alerts lost on a disk write failure |
| `archiver_stream_connected` | gauge | 1 while the Vigil stream is open |
| `archiver_stream_disconnects_total{reason}` | counter | Stream losses and failed connections. `reason`: `closed`, `timeout`, `http_error`, `connection_error` |
| `archiver_maintenance_runs_total{result}` | counter | Maintenance runs. `result`: `success`, `failure` |
| `archiver_maintenance_last_success_timestamp_seconds` | gauge | Unix time of the last successful maintenance |
| `archiver_zip_failures_total` | counter | Day folders that could not be zipped |
| `archiver_archive_bytes` | gauge | Archive size, computed at each scrape |
| `archiver_disk_free_bytes` | gauge | Free space on the archive file system |

The standard `process_*` and `python_*` metrics are also exposed. `process_start_time_seconds` shows restarts.

Suggested alert: `time() - archiver_last_alert_timestamp_seconds > 600` means no alert archived for 10 minutes.

## Docker

```bash
mkdir data && sudo chown 10001 data
docker compose up -d --build
docker compose logs -f archiver
```

`./data` is mounted on `/data`, so the archive is in `./data/archive`. The container runs as UID 10001. Set `VIGIL_STREAM_URL` to reach a Vigil that does not run on the same host.

## Local run and tests

```bash
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python main.py --url http://localhost:8000/stream --archive-dir ./archive
.venv/bin/python -m pytest
```
