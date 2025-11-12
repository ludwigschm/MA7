"""Idempotent recording orchestration helpers."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional, Protocol

from .logging import get_logger

__all__ = [
    "DeviceClient",
    "RecordingHttpError",
    "RecordingController",
    "recording_session",
]


class DeviceClient(Protocol):
    """Protocol describing the minimal client surface needed for recording."""

    async def recording_start(self, *, label: str | None = None) -> None: ...

    async def recording_begin(self) -> Any: ...

    async def recording_stop(self) -> None: ...

    async def recording_cancel(self) -> None: ...

    async def is_recording(self) -> bool: ...


@dataclass(slots=True)
class RecordingHttpError(RuntimeError):
    """HTTP-style error raised by :class:`RecordingController` clients."""

    status: int
    message: str
    transient: bool = False

    def is_transient(self) -> bool:
        return self.transient or 500 <= self.status < 600


class RecordingController:
    """High-level coordination for starting/stopping recordings."""

    def __init__(self, client: DeviceClient, logger: Optional[logging.Logger] = None) -> None:
        self._client = client
        self._log = logger or get_logger("core.recording")
        self._active = False

    async def ensure_started(self, label: str | None = None) -> None:
        """Ensure that a recording session is active."""

        if await self._client.is_recording():
            self._active = True
            self._log.info("recording already active")
            return

        delay_s = 0.2
        for attempt in range(3):
            try:
                await self._client.recording_start(label=label)
            except RecordingHttpError as exc:
                lowered = exc.message.lower()
                if exc.status == 400 and "already recording" in lowered:
                    self._log.info("recording already active (400)")
                    self._active = True
                    return
                if exc.is_transient() and attempt < 2:
                    self._log.warning("recording start retry %d", attempt + 1)
                    await asyncio.sleep(delay_s)
                    delay_s *= 2
                    continue
                raise
            except asyncio.TimeoutError:
                if attempt < 2:
                    self._log.warning("recording start retry %d", attempt + 1)
                    await asyncio.sleep(delay_s)
                    delay_s *= 2
                    continue
                raise
            else:
                self._log.info("recording start ok")
                self._active = True
                return

        raise RecordingHttpError(503, "recording start retries exhausted", transient=True)

    async def begin_segment(self, deadline_ms: int = 500) -> Any:
        """Trigger a recording segment begin event with a strict timeout."""

        if not self._active:
            return None
        try:
            info = await asyncio.wait_for(
                self._client.recording_begin(), timeout=deadline_ms / 1000
            )
        except asyncio.TimeoutError:
            self._log.warning("recording begin timeout; best-effort continue")
            return None
        else:
            self._log.info("recording begin ok")
            return info

    async def stop(self) -> None:
        """Stop the recording if active."""

        if not self._active:
            return
        await self._client.recording_stop()
        self._log.info("recording stop ok")
        self._active = False

    async def is_recording(self) -> bool:
        """Return cached or live recording state."""

        if not self._active:
            self._active = await self._client.is_recording()
        return self._active

    async def cancel(self) -> None:
        """Abort the current recording, discarding captured data if possible."""

        try:
            await self._client.recording_cancel()
        finally:
            if self._active:
                self._log.info("recording cancel ok")
            self._active = False


@asynccontextmanager
async def recording_session(bridge: Any, player: str, label: str) -> AsyncIterator[Any]:
    """Ensure events run while the device is actively recording."""

    recording_id = await bridge.recording_start(player, label=label)
    await bridge.recording_begin(player)
    try:
        yield recording_id
    finally:
        await bridge.recording_stop_and_save(player)
