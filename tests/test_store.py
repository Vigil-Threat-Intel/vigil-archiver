import asyncio
import json
import zipfile
from datetime import datetime, timezone

import pytest

from src.store import AlertStore


def alert(timestamp: str, domain: str = "a.example.xyz") -> str:
    return json.dumps({"timestamp": timestamp, "domain": domain, "score": 80})


def clock_at(text: str):
    moment = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    return lambda: moment


def test_write_partitions_by_alert_timestamp_in_utc(tmp_path):
    store = AlertStore(tmp_path, clock=clock_at("2026-11-24T20:00:00"))
    line = alert("2026-11-24T14:05:00+02:00")
    asyncio.run(store.write(line))
    target = tmp_path / "2026-11-24" / "12.jsonl"
    assert target.read_text() == line + "\n"
    assert store.written == 1


def test_write_keeps_line_unchanged_and_appends(tmp_path):
    store = AlertStore(tmp_path)
    first, second = alert("2026-11-24T14:00:00+00:00", "a.xyz"), alert("2026-11-24T14:59:00+00:00", "b.xyz")

    async def go():
        await store.write(first)
        await store.write(second)

    asyncio.run(go())
    assert (tmp_path / "2026-11-24" / "14.jsonl").read_text() == f"{first}\n{second}\n"


@pytest.mark.parametrize("bad", ["not json", "[1]", json.dumps({"domain": "x"}), json.dumps({"timestamp": "nope"})])
def test_write_rejects_unusable_alert(tmp_path, bad):
    store = AlertStore(tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(store.write(bad))
    assert list(tmp_path.iterdir()) == []
    assert store.written == 0


def test_maintain_zips_finished_days_only(tmp_path):
    (tmp_path / "2026-11-23").mkdir()
    (tmp_path / "2026-11-23" / "10.jsonl").write_text("x\n")
    (tmp_path / "2026-11-24").mkdir()
    (tmp_path / "2026-11-24" / "10.jsonl").write_text("y\n")
    AlertStore(tmp_path, clock=clock_at("2026-11-24T12:00:00")).maintain()
    assert not (tmp_path / "2026-11-23").exists()
    with zipfile.ZipFile(tmp_path / "2026-11-23.zip") as zf:
        assert zf.read("10.jsonl") == b"x\n"
    assert (tmp_path / "2026-11-24" / "10.jsonl").exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_maintain_merges_late_alerts_without_losing_entries(tmp_path):
    with zipfile.ZipFile(tmp_path / "2026-11-23.zip", "w") as zf:
        zf.writestr("10.jsonl", "old10\n")
        zf.writestr("11.jsonl", "old11\n")
    (tmp_path / "2026-11-23").mkdir()
    (tmp_path / "2026-11-23" / "10.jsonl").write_text("late10\n")
    (tmp_path / "2026-11-23" / "12.jsonl").write_text("late12\n")
    AlertStore(tmp_path, clock=clock_at("2026-11-24T12:00:00")).maintain()
    with zipfile.ZipFile(tmp_path / "2026-11-23.zip") as zf:
        assert sorted(zf.namelist()) == ["10.jsonl", "11.jsonl", "12.jsonl"]
        assert zf.read("10.jsonl") == b"old10\nlate10\n"
        assert zf.read("11.jsonl") == b"old11\n"
        assert zf.read("12.jsonl") == b"late12\n"
    assert not (tmp_path / "2026-11-23").exists()


def test_maintain_purges_old_zips(tmp_path):
    for name in ("2026-10-01.zip", "2026-10-25.zip", "2026-11-23.zip"):
        with zipfile.ZipFile(tmp_path / name, "w") as zf:
            zf.writestr("00.jsonl", "")
    AlertStore(tmp_path, retention_days=30, clock=clock_at("2026-11-24T12:00:00")).maintain()
    assert sorted(p.name for p in tmp_path.glob("*.zip")) == ["2026-10-25.zip", "2026-11-23.zip"]


def test_maintain_ignores_missing_directory(tmp_path):
    AlertStore(tmp_path / "absent").maintain()


def test_corrupt_zip_keeps_folder_for_retry(tmp_path):
    (tmp_path / "2026-11-23.zip").write_bytes(b"garbage")
    (tmp_path / "2026-11-23").mkdir()
    (tmp_path / "2026-11-23" / "10.jsonl").write_text("x\n")
    AlertStore(tmp_path, clock=clock_at("2026-11-24T12:00:00")).maintain()
    assert (tmp_path / "2026-11-23" / "10.jsonl").read_text() == "x\n"
    assert not list(tmp_path.glob("*.tmp"))
