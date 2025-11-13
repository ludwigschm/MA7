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
from .clock import now_ns, now_mono_ns

__all__ = [
    "AsyncSimpleDeviceWrapper",
    "AtomicRef",
    "OffsetState",
    "TimeSyncHealthSampler",
    "TimeSyncManager",
    "TimeSyncSampleError",
    "TimeSyncSample",
    "TimeSyncResult",
    "OfficialTimeSync",
    "make_timesync",
    "get_offset_ns",
    "get_offset_ns_for_event",
    "get_health",
    "get_rms_ms",
    "get_state",
    "should_swap",
]


class AsyncSimpleDeviceWrapper:
    """Adapt a Pupil Labs Simple API device for safe async usage.

    The Simple API exposes :meth:`estimate_time_offset` as a synchronous method
    that internally calls :func:`asyncio.run`.  When invoked from within an
    existing event loop this raises ``RuntimeError: asyncio.run() cannot be
    called from a running event loop``.  Wrapping the device allows the method
    to execute inside a worker thread while presenting an ``async`` interface to
    the rest of the time-sync stack.
    """

    def __init__(self, device: Any) -> None:
        self._device = device

    def _estimate_blocking(self, *args: Any, **kwargs: Any) -> Any:
        result = self._device.estimate_time_offset(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def estimate_time_offset(self, *args: Any, **kwargs: Any) -> Any:
        """Run the blocking Simple-API method inside the default executor."""

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._estimate_blocking,
            *args,
            **kwargs,
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the wrapped device."""

        return getattr(self._device, name)

RESYNC_INTERVAL_S = float(TIMESYNC_RESYNC_INTERVAL_S)
DRIFT_THRESHOLD_S = float(TIMESYNC_DRIFT_THRESHOLD_MS) / 1000.0
MAX_SAMPLES = 64
RMS_REFINE_THRESHOLD_S = 0.0015
ABSURD_OFFSET_NS = 5_000_000_000
STABLE_RMS_THRESHOLD_MS = 2.0
STABLE_JUMP_THRESHOLD_MS = 3.0


class TimeSyncSampleError(RuntimeError):
    """Raised when a time synchronisation measurement fails."""


@dataclass(frozen=True)
class TimeSyncSample:
    offset_ms: float
    rtt_ms: float
    accepted: bool
    reason: str | None = None


@dataclass(frozen=True)
class TimeSyncResult:
    median_offset_ms: float
    iqr_ms: float
    rms_rtt_ms: float
    accepted: int
    total: int


@dataclass(frozen=True)
class OffsetState:
    median_ns: int
    rms_ns: int
    seq: int
    updated_at_ns: int


@dataclass(frozen=True)
class HealthState:
    rms_ms: float
    offset_jump_ms_last: float
    is_stable: bool


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
        updated_at_ns=now_ns(),
    )
)
_last_update_mono_ns = AtomicRef(now_mono_ns())
_absurd_logged = AtomicRef(False)
_health_state = AtomicRef(
    HealthState(
        rms_ms=0.0,
        offset_jump_ms_last=0.0,
        is_stable=True,
    )
)


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


def get_health() -> dict[str, float | bool]:
    """Return the current time synchronisation health state."""

    health = _health_state.load()
    return {
        "rms_ms": health.rms_ms,
        "offset_jump_ms_last": health.offset_jump_ms_last,
        "is_stable": health.is_stable,
    }


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
        now_mono = now_mono_ns()
        if observed_drift_s is not None and abs(observed_drift_s) <= self.drift_threshold_s:
            return state.median_ns / 1_000_000_000.0
        if (
            observed_drift_s is None
            and (
                now_mono - _last_update_mono_ns.load()
                < int(self.resync_interval_s * 1_000_000_000)
            )
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
                    "TimeSync measurement failed for %s: %s",
                    self.device_id,
                    exc,
                )
                state = _offset_state.load()
                prev_health = _health_state.load()
                current_rms_ms = state.rms_ns / 1_000_000.0
                _health_state.store(
                    HealthState(
                        rms_ms=current_rms_ms,
                        offset_jump_ms_last=prev_health.offset_jump_ms_last,
                        is_stable=
                        abs(current_rms_ms) <= STABLE_RMS_THRESHOLD_MS
                        and abs(prev_health.offset_jump_ms_last) <= STABLE_JUMP_THRESHOLD_MS,
                    )
                )
                return state.median_ns, state.rms_ns

            old_state = _offset_state.load()
            jump_ms = 0.0
            if old_state.seq != 0:
                jump_ms = (new_state.median_ns - old_state.median_ns) / 1_000_000.0
            rms_ms = new_state.rms_ns / 1_000_000.0
            is_stable = abs(rms_ms) <= STABLE_RMS_THRESHOLD_MS and abs(jump_ms) <= STABLE_JUMP_THRESHOLD_MS
            _health_state.store(
                HealthState(
                    rms_ms=rms_ms,
                    offset_jump_ms_last=jump_ms,
                    is_stable=is_stable,
                )
            )
            if should_swap(old_state, new_state):
                _offset_state.store(new_state)
                if abs(new_state.median_ns) <= ABSURD_OFFSET_NS:
                    _absurd_logged.store(False)
                median_ms = new_state.median_ns / 1_000_000.0
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
            updated_at_ns=now_ns(),
        )
        _last_update_mono_ns.store(now_mono_ns())
        return new_state, len(offsets_ns)

    def _ensure_loop_running(self) -> None:
        if self._resync_task is None or self._resync_task.done():
            self._stopped.clear()
            self._resync_task = asyncio.create_task(self._run_resync_loop())


class OfficialTimeSync:
    def __init__(
        self,
        device: Any,
        device_key: str,
        min_samples: int = 10,
        max_samples: int = 40,
        rtt_ms_max: float = 120.0,
        offset_ms_abs_max: float = 250.0,
        inter_sample_sleep_s: float = 0.03,
    ) -> None:
        self.device = device
        self.device_key = device_key
        self.min_samples = int(min_samples)
        self.max_samples = int(max_samples)
        self.rtt_ms_max = float(rtt_ms_max)
        self.offset_ms_abs_max = float(offset_ms_abs_max)
        self.inter_sample_sleep_s = float(inter_sample_sleep_s)
        self._log = logging.getLogger(f"core.time_sync.{device_key}")

    async def measure_once(self) -> TimeSyncSample | None:
        est = await self.device.estimate_time_offset()
        if est is None:
            return TimeSyncSample(0.0, 0.0, False, "estimator_none")
        try:
            offset_ms = float(est.time_offset_ms.mean)
            rtt_ms = float(est.roundtrip_duration_ms.mean)
        except Exception as exc:  # pragma: no cover - estimator contract
            return TimeSyncSample(0.0, 0.0, False, f"estimator_bad:{exc!s}")
        reason = None
        accepted = True
        if abs(offset_ms) > self.offset_ms_abs_max:
            accepted = False
            reason = "abs_offset"
        elif rtt_ms <= 0 or rtt_ms > self.rtt_ms_max:
            accepted = False
            reason = "rtt_range"
        return TimeSyncSample(offset_ms, rtt_ms, accepted, reason)

    async def calibrate(self) -> TimeSyncResult:
        samples: list[TimeSyncSample] = []
        while len(samples) < self.max_samples:
            sample = await self.measure_once()
            if sample is not None:
                samples.append(sample)
            await asyncio.sleep(self.inter_sample_sleep_s)

        accepted = [sample for sample in samples if sample.accepted]
        if len(accepted) < self.min_samples:
            self._log_tail(samples)
            if len(accepted) == 0:
                raise RuntimeError("TimeSync: no valid samples")
            raise RuntimeError(
                f"TimeSync: insufficient valid samples {len(accepted)}/{len(samples)}"
            )

        median_offset = statistics.median(sample.offset_ms for sample in accepted)
        sorted_offsets = sorted(sample.offset_ms for sample in accepted)
        half = len(sorted_offsets) // 2
        q1 = statistics.median(sorted_offsets[:half])
        q3 = statistics.median(sorted_offsets[(len(sorted_offsets) + 1) // 2 :])
        iqr = q3 - q1
        rms_rtt = (sum(sample.rtt_ms**2 for sample in accepted) / len(accepted)) ** 0.5

        self._log_summary(median_offset, iqr, rms_rtt, len(accepted), len(samples))
        return TimeSyncResult(median_offset, iqr, rms_rtt, len(accepted), len(samples))

    def apply_ns(self, device_ts_ns: int, median_offset_ms: float) -> int:
        return int(device_ts_ns + median_offset_ms * 1e6)

    def _log_tail(self, samples: list[TimeSyncSample]) -> None:
        for sample in samples[-5:]:
            status = "ok" if sample.accepted else "reject:" + str(sample.reason)
            message = (
                f"TIMESYNC_SAMPLE device={self.device_key} rtt_ms={sample.rtt_ms:.2f} "
                f"offset_ms={sample.offset_ms:.2f} {status}"
            )
            if sample.accepted:
                if self._log.isEnabledFor(logging.DEBUG):
                    self._log.debug(message)
            else:
                self._log.warning(message)

    def _log_summary(
        self,
        median_ms: float,
        iqr: float,
        rms_rtt_ms: float,
        accepted: int,
        total: int,
    ) -> None:
        self._log.info(
            "TIMESYNC_SUMMARY device=%s median_ms=%.2f iqr_ms=%.2f "
            "rms_rtt_ms=%.2f accepted=%d/%d",
            self.device_key,
            median_ms,
            iqr,
            rms_rtt_ms,
            accepted,
            total,
        )


def make_timesync(device: Any, device_key: str, **kwargs: Any) -> OfficialTimeSync:
    return OfficialTimeSync(device, device_key, **kwargs)


def get_offset_ns_for_event() -> int:
    """Backwards-compatible alias for event senders."""

    return get_offset_ns()


class TimeSyncHealthSampler:
    """Periodically sample the current time-sync health for diagnostics."""

    def __init__(
        self,
        device_id: str,
        *,
        interval_s: float = 5.0,
        warn_interval_s: float = 10.0,
        event_emitter: Optional[Callable[[str, dict[str, Any]], None]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.device_id = device_id
        self.interval_s = float(interval_s)
        self.warn_interval_s = float(warn_interval_s)
        self._event_emitter = event_emitter
        self._log = logger or get_logger(f"core.time_sync.health.{device_id}")
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._last_warn_monotonic = 0.0
        self._last_stable: Optional[bool] = None

    async def start(self) -> None:
        """Start sampling in the background."""

        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the background sampler."""

        if not self._task:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                self._sample()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_s)
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise

    def _sample(self) -> None:
        health = _health_state.load()
        state = _offset_state.load()
        rms_ms = float(health.rms_ms)
        jump_ms = float(health.offset_jump_ms_last)
        is_stable = bool(health.is_stable)
        now = time.monotonic()

        exceeds_warn = abs(rms_ms) > 3.0 or abs(jump_ms) > 5.0
        if exceeds_warn and (now - self._last_warn_monotonic) >= self.warn_interval_s:
            self._log.warning(
                "TimeSync instability detected device=%s rms_ms=%.3f jump_ms=%.3f",
                self.device_id,
                rms_ms,
                jump_ms,
            )
            self._last_warn_monotonic = now

        if self._last_stable is None:
            self._last_stable = is_stable
        elif self._last_stable != is_stable:
            self._last_stable = is_stable
            payload = {
                "device_id": self.device_id,
                "stable": is_stable,
                "rms_ms": rms_ms,
                "offset_jump_ms_last": jump_ms,
                "offset_ns": state.median_ns,
            }
            emitter = self._event_emitter
            if emitter is not None:
                try:
                    emitter("timesync.recalibrated", payload)
                except Exception:
                    self._log.exception("TimeSync health event emission failed")
