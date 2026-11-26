"""Hourly JSON Lines archive with daily zip rotation and retention purge."""

import asyncio
import json
import logging
import os
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from src import metrics

logger = logging.getLogger("archiver.store")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AlertStore:
    """Append alerts to ``YYYY-MM-DD/HH.jsonl``, zip finished days, purge old zips."""

    def __init__(
        self,
        directory: str | Path,
        retention_days: int = 30,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._directory = Path(directory)
        self._retention_days = retention_days
        self._clock = clock
        self._lock = asyncio.Lock()
        self.written = 0

    @staticmethod
    def partition(alert: str) -> datetime:
        """Return the UTC moment of an alert from its ``timestamp`` field; raise ``ValueError`` if unusable."""
        data = json.loads(alert)
        if not isinstance(data, dict):
            raise ValueError("alert is not a JSON object")
        raw = data.get("timestamp")
        if not isinstance(raw, str):
            raise ValueError("alert has no timestamp")
        moment = datetime.fromisoformat(raw)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc)

    async def write(self, alert: str) -> None:
        """Append one alert, unchanged, to the file of its hour."""
        moment = self.partition(alert)
        async with self._lock:
            await asyncio.to_thread(self._append, alert, moment)
        self.written += 1
        metrics.ALERTS_WRITTEN.inc()
        metrics.LAST_ALERT.set_to_current_time()

    def _append(self, alert: str, moment: datetime) -> None:
        day_dir = self._directory / moment.strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        with (day_dir / f"{moment:%H}.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(alert + "\n")

    def maintain(self) -> None:
        """Zip every finished day folder, then purge zips older than the retention."""
        if not self._directory.is_dir():
            return
        today = self._clock().date()
        for entry in sorted(self._directory.iterdir()):
            day = self._parse_day(entry.name)
            if entry.is_dir() and day is not None and day < today:
                self._zip_day(entry)
        self._purge(today)

    @staticmethod
    def _parse_day(name: str) -> date | None:
        try:
            return date.fromisoformat(name)
        except ValueError:
            return None

    def _zip_day(self, day_dir: Path) -> None:
        target = day_dir.with_name(day_dir.name + ".zip")
        tmp = day_dir.with_name(day_dir.name + ".zip.tmp")
        hour_files = sorted(day_dir.glob("*.jsonl"))
        try:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as new:
                merged = self._copy_existing(target, day_dir, new)
                for hour_file in hour_files:
                    if hour_file.name not in merged:
                        new.write(hour_file, arcname=hour_file.name)
            os.replace(tmp, target)
        except Exception:
            tmp.unlink(missing_ok=True)
            metrics.ZIP_FAILURES.inc()
            logger.exception("Zipping %s failed", day_dir)
            return
        for hour_file in hour_files:
            hour_file.unlink()
        try:
            day_dir.rmdir()
        except OSError:
            logger.warning("%s not empty after zipping, folder kept", day_dir)
        logger.info("Archived %s", target.name)

    @staticmethod
    def _copy_existing(target: Path, day_dir: Path, new: zipfile.ZipFile) -> set[str]:
        """Copy the entries of an existing zip, appending late alerts of the same hour; return the names handled."""
        handled: set[str] = set()
        if not target.exists():
            return handled
        with zipfile.ZipFile(target) as old:
            for name in old.namelist():
                data = old.read(name)
                late = day_dir / name
                if late.is_file():
                    data += late.read_bytes()
                new.writestr(name, data)
                handled.add(name)
        return handled

    def _purge(self, today: date) -> None:
        limit = today - timedelta(days=self._retention_days)
        for zip_path in self._directory.glob("*.zip"):
            day = self._parse_day(zip_path.stem)
            if day is not None and day < limit:
                zip_path.unlink(missing_ok=True)
                logger.info("Purged %s", zip_path.name)

    async def run_maintenance(self, interval: float = 3600.0) -> None:
        """Run ``maintain`` now, then every ``interval`` seconds, never overlapping a write."""
        while True:
            try:
                async with self._lock:
                    await asyncio.to_thread(self.maintain)
                metrics.MAINTENANCE_RUNS.labels("success").inc()
                metrics.MAINTENANCE_LAST_SUCCESS.set_to_current_time()
            except Exception:
                metrics.MAINTENANCE_RUNS.labels("failure").inc()
                logger.exception("Archive maintenance failed")
            await asyncio.sleep(interval)
