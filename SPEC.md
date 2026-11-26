# vigil-archiver: specification

## Purpose

Read the live alert stream exposed by Vigil (`GET /stream`, Server-Sent Events) and archive every alert on local disk. Nothing else: no search, no notification, no enrichment.

Vigil stores nothing by design. This service owns the storage.

## Decisions

| Topic | Decision |
|---|---|
| Language / runtime | Python 3.12, Docker image |
| Delivery guarantee | Loss tolerated. Alerts emitted while the archiver is down or disconnected are lost. No change on the Vigil side. |
| Storage | Local files: hourly JSON Lines, daily zip |
| Partition clock | `timestamp` field of the alert, UTC |
| Line content | The JSON received from Vigil, written unchanged |
| Deduplication | None. Every alert is stored as received. |
| Retention | Zips older than `RETENTION_DAYS` are deleted (default 30) |
| Reconnection | Fixed 5 s delay |
| Health check | None. Observability through stdout logs and a Prometheus `/metrics` endpoint (port `METRICS_PORT`, default 9108). The metric list is in the README. |
| Deployment | Separate docker compose, targets Vigil by URL |

## Input

- Source: `VIGIL_STREAM_URL`, default `http://host.docker.internal:8000/stream`.
- Keep only the lines that start with `data:`. Ignore the SSE comments `: connected` and `: keepalive`.
- Reconnect after any disconnection or error, with a fixed 5 s delay.
- Read timeout: if no byte is received for 45 s, close the connection and reconnect. Vigil sends a keepalive every 15 s, so 45 s means the link is dead. Without this timeout a silent network cut would block the archiver forever, since there is no health check.
- A line that is not valid JSON, or that has no valid `timestamp`, is logged at `WARNING` and skipped. The archiver never crashes on bad input.

Alert format sent by Vigil:

```json
{"timestamp": "2026-11-24T11:15:54.732429+00:00", "domain": "login.paypa1-secure.xyz", "root": "paypa1-secure", "subdomain": "login", "tld": "xyz", "score": 80, "reasons": ["typosquatting: paypal"], "issuer": "Let's Encrypt", "serial": "04AB...", "cert_type": "X509"}
```

## Storage layout

```
/data/archive/
├── 2026-11-24/            current day
│   └── 14.jsonl
├── 2026-11-23.zip         finished day: one HH.jsonl per entry
└── 2026-11-22.zip
```

- The file is chosen from the alert `timestamp`, converted to UTC: `YYYY-MM-DD/HH.jsonl`.
- Write: append the line and flush after each alert.
- A late alert whose hour belongs to an already zipped day goes into a new `YYYY-MM-DD/HH.jsonl` folder. The next maintenance run merges it into the existing zip, without overwriting entries already in it.

### Maintenance

Runs at startup, then every hour:

1. Zip every day folder older than today into `YYYY-MM-DD.zip`, then delete the folder. This also catches up on days left as folders after an unclean shutdown.
2. Write the zip as `.zip.tmp`, then `os.replace` it onto the final name, so a crash never leaves a half-written zip.
3. Delete zips older than `RETENTION_DAYS`.
4. A failure in one day is logged and does not stop the others.

Reference implementation: the former Vigil archiver (`src/output/archiver.py`, removed from Vigil). Recover it with `git show 0b6945f:src/output/archiver.py` in the Vigil repository and reuse its zip, merge and purge logic. Its `write()` must change to take the partition hour from the alert `timestamp` instead of the clock.

## Configuration

| Variable | CLI option | Default | Role |
|---|---|---|---|
| `VIGIL_STREAM_URL` | `--url` | `http://host.docker.internal:8000/stream` | Vigil endpoint |
| `ARCHIVE_DIR` | `--archive-dir` | `/data/archive` | Archive root |
| `RETENTION_DAYS` | `--retention-days` | `30` | Days of zips kept |
| `METRICS_PORT` | `--metrics-port` | `9108` | Prometheus `/metrics` port, `0` disables it |
| none | `--verbose` | off | DEBUG logs |

The CLI option wins over the environment variable.

## Logging

Stdout only, standard `logging` format.

- `INFO`: connected, connection lost (with the reason), retry, archived day, purged zip.
- `INFO`, once per minute: number of alerts written during the last minute.
- `WARNING`: invalid line skipped.
- `ERROR`: write or zip failure, with the traceback.

## Deployment

- Image: `python:3.12-slim`, non-root user (UID 10001), `PYTHONUNBUFFERED=1`.
- Dependencies: `aiohttp` (async HTTP client with a read timeout, also serves `/metrics`), `prometheus_client`.
- Separate `docker-compose.yml`:
  - `restart: unless-stopped`, because every minute offline is lost data.
  - `extra_hosts: ["host.docker.internal:host-gateway"]` to reach the port Vigil publishes on the host.
  - Bind mount `./data:/data`. Prepare it once: `mkdir data && sudo chown 10001 data`.
- `data/` goes in `.gitignore`.

## Out of scope

Search or query API, notifications, enrichment, deduplication, replay without loss (`Last-Event-ID`), authentication, health check.

## Acceptance checks

1. Start a fake stream (or Vigil with `--threshold 0`) and the archiver: `data/archive/<today>/<hour>.jsonl` fills, one valid JSON line per alert, identical to what Vigil sent.
2. Stop Vigil: the archiver logs the disconnection and retries every 5 s. Restart Vigil: it reconnects without a restart of the archiver.
3. Cut the network silently (for example a stalled proxy): the archiver reconnects after 45 s.
4. Leave a past-day folder in `data/archive/` and restart: it is zipped at startup and the folder is deleted.
5. Drop a zip older than `RETENTION_DAYS`: it is deleted at the next maintenance run.
6. Send a malformed line: one warning, the archiver keeps running.
7. `docker compose up -d` then `docker compose exec archiver ls /data/archive`: the files are written as UID 10001 and are readable from the host.
