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
| none | `--verbose` | off | DEBUG logs |

The CLI option wins over the environment variable.

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
