"""Asynchronous device/host time synchronisation utilities."""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

import metrics
from .config import TIMESYNC_DRIFT_THRESHOLD_MS, TIMESYNC_RESYNC_INTERVAL_S
from .logging import get_logger

__all__ = [
    "AtomicRef",
    "OffsetState",
    "TimeSyncManager",
    "TimeSyncSampleError",
    "get_offset_ns",
    "get_offset_ns_for_event",
    "get_rms_ms",
    "get_state",
    "should_swap",
]

RESYNC_INTERVAL_S = float(TIMESYNC_RESYNC_INTERVAL_S)
DRIFT_THRESHOLD_S = float(TIMESYNC_DRIFT_THRESHOLD_MS) / 1000.0
MAX_SAMPLES = 64
RMS_REFINE_THRESHOLD_S = 0.0015
ABSURD_OFFSET_NS = 5_000_000_000


class TimeSyncSampleError(RuntimeError):
    """Raised when a time synchronisation measurement fails."""


@dataclass(frozen=True)
class OffsetState:
    median_ns: int
    rms_ns: int
    seq: int
    updated_at_mono_ns: int


class AtomicRef:
    """A minimal atomic reference relying on the GIL for readers."""

    def __init__(self, value: Any) -> None:
        self._value = value
        self._lock = threading.Lock()

    def load(self) -> Any:
        return self._value

    def store(self, value: Any) -> None:
        with self._lock:
            self._value = value


_offset_state = AtomicRef(
    OffsetState(
        median_ns=0,
        rms_ns=0,
        seq=0,
        updated_at_mono_ns=time.monotonic_ns(),
    )
)
_absurd_logged = AtomicRef(False)


def get_state() -> OffsetState:
    """Return the current immutable offset state."""

    return _offset_state.load()


def get_offset_ns() -> int:
    """Return the current median offset in nanoseconds without blocking."""

    state = _offset_state.load()
    offset = state.median_ns
    if abs(offset) > ABSURD_OFFSET_NS:
        if not _absurd_logged.load():
            _absurd_logged.store(True)
            logger = get_logger("core.time_sync")
            logger.warning("TimeSync: absurd offset detected offset_ns=%d", offset)
        return 0
    return offset


def get_rms_ms() -> float:
    """Return the RMS jitter in milliseconds."""

    return _offset_state.load().rms_ns / 1_000_000.0


def should_swap(old: OffsetState, new: OffsetState) -> bool:
    """Determine whether a newly measured state should replace the old one."""

    if old.seq == 0:
        return True
    drift_threshold_ns = int(DRIFT_THRESHOLD_S * 1_000_000_000)
    rms_threshold_ns = int(RMS_REFINE_THRESHOLD_S * 1_000_000_000)
    if abs(new.median_ns - old.median_ns) > drift_threshold_ns:
        return True
    if new.rms_ns > rms_threshold_ns:
        return True
    return False


class TimeSyncManager:
    """Continuously estimate device-to-host time offset without blocking readers.

    Readers (event senders) observe the offset via :func:`get_offset_ns`, which
    performs a single atomic snapshot without acquiring locks.  Writers (resync
    tasks) replace the shared :class:`OffsetState` atomically after completing a
    full measurement cycle, ensuring event logging remains non-blocking even
    during resynchronisation.
    """

    def __init__(
        self,
        device_id: str,
        measure_fn: Callable[[int, float], Awaitable[list[float]]],
        *,
        max_samples: int = MAX_SAMPLES,
        sample_timeout: float = 0.25,
        resync_interval_s: float = RESYNC_INTERVAL_S,
        drift_threshold_s: float = DRIFT_THRESHOLD_S,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.device_id = device_id
        self._measure_fn = measure_fn
        self.max_samples = max_samples
        self.sample_timeout = sample_timeout
        self._log = logger or get_logger(f"core.time_sync.{device_id}")
        self._lock = asyncio.Lock()
        self._resync_task: Optional[asyncio.Task[None]] = None
        self._stopped = asyncio.Event()
        self.resync_interval_s = float(resync_interval_s)
        self.drift_threshold_s = float(drift_threshold_s)

    def get_offset_ns(self) -> int:
        return get_offset_ns()

    def get_offset_s(self) -> float:
        return self.get_offset_ns() / 1_000_000_000.0

    async def start(self) -> None:
        """Kick off the continuous resynchronisation loop."""

        await self.force_resync()
        self._ensure_loop_running()

    async def stop(self) -> None:
        """Stop the resynchronisation loop."""

        self._stopped.set()
        if self._resync_task:
            self._resync_task.cancel()
            try:
                await self._resync_task
            except asyncio.CancelledError:
                pass
            self._resync_task = None

    async def initial_sync(self) -> float:
        """Perform the first synchronisation pass and start background updates."""

        median_ns, _ = await self.force_resync()
        self._ensure_loop_running()
        return median_ns / 1_000_000_000.0

    async def maybe_resync(self, observed_drift_s: float | None = None) -> float:
        """Compatibility helper that triggers a resync when drift exceeds thresholds."""

        state = _offset_state.load()
        now_ns = time.monotonic_ns()
        if observed_drift_s is not None and abs(observed_drift_s) <= self.drift_threshold_s:
            return state.median_ns / 1_000_000_000.0
        if (
            observed_drift_s is None
            and (now_ns - state.updated_at_mono_ns) < int(self.resync_interval_s * 1_000_000_000)
        ):
            return state.median_ns / 1_000_000_000.0
        median_ns, _ = await self.force_resync()
        return median_ns / 1_000_000_000.0

    async def force_resync(self) -> tuple[int, int]:
        """Perform a synchronisation pass immediately.

        Returns
        -------
        tuple[int, int]
            The (median_ns, rms_ns) from the most recent successful measurement.
        """

        async with self._lock:
            try:
                new_state, sample_count = await self._collect_state()
            except TimeSyncSampleError as exc:
                self._log.warning(
                    "TimeSync: device=%s status=failed error=%s",
                    self.device_id,
                    exc,
                )
                state = _offset_state.load()
                return state.median_ns, state.rms_ns

            old_state = _offset_state.load()
            if should_swap(old_state, new_state):
                _offset_state.store(new_state)
                if abs(new_state.median_ns) <= ABSURD_OFFSET_NS:
                    _absurd_logged.store(False)
                median_ms = new_state.median_ns / 1_000_000.0
                rms_ms = new_state.rms_ns / 1_000_000.0
                metrics.gauge(
                    "timesync_rms_ms",
                    rms_ms,
                    device=self.device_id,
                )
                metrics.gauge(
                    "timesync_samples",
                    float(sample_count),
                    device=self.device_id,
                )
                self._log.info(
                    "TimeSync: device=%s median_offset=%.3fms rms=%.3fms samples=%d",
                    self.device_id,
                    median_ms,
                    rms_ms,
                    sample_count,
                )
            return new_state.median_ns, new_state.rms_ns

    async def _run_resync_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                await asyncio.sleep(self.resync_interval_s)
                try:
                    await self.force_resync()
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - network dependent
                    self._log.exception("TimeSync: background resync failed")
        except asyncio.CancelledError:
            pass

    async def _collect_state(self) -> tuple[OffsetState, int]:
        try:
            raw_samples = await self._measure_fn(self.max_samples, self.sample_timeout)
        except asyncio.CancelledError:  # pragma: no cover - defensive
            raise
        except Exception as exc:  # pragma: no cover - network dependent
            raise TimeSyncSampleError(str(exc)) from exc

        samples = [float(sample) for sample in raw_samples if isinstance(sample, (int, float))]
        if not samples:
            raise TimeSyncSampleError("no valid samples")

        if len(samples) > self.max_samples:
            samples = samples[: self.max_samples]
        if len(samples) > 20:
            samples = samples[:20]

        offsets_ns = [int(sample * 1_000_000_000) for sample in samples]
        median_ns = int(statistics.median(offsets_ns))
        mean_square = statistics.fmean((sample - median_ns) ** 2 for sample in offsets_ns)
        rms_ns = int(mean_square ** 0.5)
        old_state = _offset_state.load()
        new_state = OffsetState(
            median_ns=median_ns,
            rms_ns=rms_ns,
            seq=old_state.seq + 1,
            updated_at_mono_ns=time.monotonic_ns(),
        )
        return new_state, len(offsets_ns)

    def _ensure_loop_running(self) -> None:
        if self._resync_task is None or self._resync_task.done():
            self._stopped.clear()
            self._resync_task = asyncio.create_task(self._run_resync_loop())


def get_offset_ns_for_event() -> int:
    """Backwards-compatible alias for event senders."""

    return get_offset_ns()
