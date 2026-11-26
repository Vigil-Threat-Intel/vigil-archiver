"""Vigil SSE stream client with fixed-delay reconnection and a read timeout."""

import asyncio
import logging
from typing import Awaitable, Callable

import aiohttp

from src import metrics

logger = logging.getLogger("archiver.client")

DEFAULT_URL = "http://host.docker.internal:8000/stream"
RECONNECT_DELAY = 5.0
READ_TIMEOUT = 45.0


class StreamClient:
    """Pass the payload of every ``data:`` line to a callback, reconnecting forever."""

    def __init__(
        self,
        url: str,
        on_alert: Callable[[str], Awaitable[None]],
        reconnect_delay: float = RECONNECT_DELAY,
        read_timeout: float = READ_TIMEOUT,
    ) -> None:
        self._url = url
        self._on_alert = on_alert
        self._reconnect_delay = reconnect_delay
        self._timeout = aiohttp.ClientTimeout(total=None, sock_connect=10.0, sock_read=read_timeout)

    async def run(self) -> None:
        """Consume the stream forever."""
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            while True:
                try:
                    await self._consume(session)
                    metrics.STREAM_DISCONNECTS.labels("closed").inc()
                    logger.info("Stream closed by server, retrying in %ss", self._reconnect_delay)
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                    metrics.STREAM_DISCONNECTS.labels(self.disconnect_reason(exc)).inc()
                    logger.warning(
                        "Connection lost (%s: %s), retrying in %ss",
                        type(exc).__name__,
                        exc,
                        self._reconnect_delay,
                    )
                await asyncio.sleep(self._reconnect_delay)

    async def _consume(self, session: aiohttp.ClientSession) -> None:
        async with session.get(self._url) as response:
            response.raise_for_status()
            logger.info("Connected to %s", self._url)
            metrics.STREAM_CONNECTED.set(1)
            try:
                async for raw in response.content:
                    payload = self.parse_line(raw)
                    if payload is not None:
                        await self._on_alert(payload)
            finally:
                metrics.STREAM_CONNECTED.set(0)

    @staticmethod
    def disconnect_reason(exc: BaseException) -> str:
        """Map a connection error to a bounded ``reason`` label."""
        if isinstance(exc, asyncio.TimeoutError):
            return "timeout"
        if isinstance(exc, aiohttp.ClientResponseError):
            return "http_error"
        return "connection_error"

    @staticmethod
    def parse_line(raw: bytes) -> str | None:
        """Return the payload of a ``data:`` line, or ``None`` for comments, blanks and other fields."""
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if not line.startswith("data:"):
            return None
        payload = line[len("data:"):].strip()
        return payload or None
