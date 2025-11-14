"""Route UI events to the appropriate device."""

from __future__ import annotations

import functools
import logging
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Callable, Deque, Dict, Literal, Sequence

import metrics

from .config import EVENT_NORMAL_BATCH_INTERVAL_S, EVENT_NORMAL_MAX_BATCH

__all__ = [
    "Priority",
    "TimestampPolicy",
    "UIEvent",
    "EventRouter",
    "classify_event",
    "debounce",
    "policy_for",
]


log = logging.getLogger(__name__)


class Priority(IntEnum):
    """Priorities for routed events."""

    HIGH = 0
    NORMAL = 1


_HIGH_PRIORITY_PATTERN = re.compile(
    r"^(fix\.|sync\.|stimulus\.onset|trial\.start|marker\.)"
)


TimestampPolicy = Enum("TimestampPolicy", ["ARRIVAL", "CLIENT_CORRECTED"])


def classify_event(name: str) -> Priority:
    """Classify an event by name into a :class:`Priority`."""

    if _HIGH_PRIORITY_PATTERN.match(name):
        return Priority.HIGH
    return Priority.NORMAL


def policy_for(name: str) -> TimestampPolicy:
    """Return the timestamping policy to use for an event name."""

    if name.startswith(("device.", "sensor.")):
        return TimestampPolicy.ARRIVAL
    # With the legacy reconciler removed we always rely on the single measured
    # companion offset. There is no soft fallback that would re-route traffic to
    # arrival timestamps.
    return TimestampPolicy.CLIENT_CORRECTED


@dataclass(slots=True)
class UIEvent:
    """Event emitted from the UI that should be forwarded to a device."""

    name: str
    payload: dict[str, object] | None = None
    target: str | None = None
    broadcast: bool = False
    priority: Priority | Literal["high", "normal", "low"] | None = None
    timestamp_policy: TimestampPolicy = TimestampPolicy.CLIENT_CORRECTED


def debounce(name_pattern: str, window_ms: int) -> Callable[[Callable[..., object]], Callable[..., object]]:
    """Coalesce matching events occurring within ``window_ms`` milliseconds."""

    pattern = re.compile(name_pattern)
    window_s = max(0.0, window_ms / 1000.0)

    def decorator(func: Callable[..., object]) -> Callable[..., object]:
        attr_state = f"_{func.__name__}_debounce_state_{hash(pattern.pattern)}"
        attr_lock = f"{attr_state}_lock"

        @functools.wraps(func)
        def wrapper(self, event: UIEvent, *args, **kwargs):  # type: ignore[override]
            if not isinstance(event, UIEvent):
                return func(self, event, *args, **kwargs)

            candidate_name = event.name
            if pattern.match(candidate_name):
                matched = True
            elif "." in candidate_name:
                prefix = candidate_name.split(".", 1)[0] + "."
                matched = bool(pattern.match(prefix))
            else:
                matched = False

            if not matched:
                return func(self, event, *args, **kwargs)

            if window_s <= 0:
                return func(self, event, *args, **kwargs)

            state: Dict[tuple[object, ...], threading.Timer] = getattr(self, attr_state, None)
            if state is None:
                state = {}
                setattr(self, attr_state, state)

            lock: threading.Lock = getattr(self, attr_lock, None)
            if lock is None:
                lock = threading.Lock()
                setattr(self, attr_lock, lock)

            key = (
                event.name,
                event.target,
                event.broadcast,
                event.priority,
                event.timestamp_policy,
            )
            call_args = args
            call_kwargs = dict(kwargs)

            timer: threading.Timer | None = None

            def dispatch() -> None:
                with lock:
                    current = state.get(key)
                    if current is not timer:
                        return
                    state.pop(key, None)
                func(self, event, *call_args, **call_kwargs)

            with lock:
                existing = state.get(key)
                if existing is not None:
                    existing.cancel()
                    if hasattr(self, "events_coalesced_total"):
                        self.events_coalesced_total += 1  # type: ignore[attr-defined]
                        metrics.inc("events_coalesced_total")
                timer = threading.Timer(window_s, dispatch)
                state[key] = timer
                timer.daemon = True
                timer.start()
            return None

        return wrapper

    return decorator


class EventRouter:
    """Route events to devices with optional batching to limit traffic."""

    _NORMAL_MAX_DEPTH = 128

    def __init__(
        self,
        deliver: Callable[[str, UIEvent], None],
        *,
        normal_batch_interval_s: float | None = None,
        normal_max_batch: int | None = None,
        batch_interval_s: float | None = None,
        max_batch: int | None = None,
        multi_route: bool = False,
    ) -> None:
        self._deliver = deliver
        self._multi_route = multi_route
        self._active_player: str | None = None
        self._known_players: set[str] = set()
        self._normal_queues: Dict[str, Deque[UIEvent]] = {}
        self._normal_timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self.events_high_total = 0
        self.events_normal_total = 0
        self.events_coalesced_total = 0
        self.normal_batches_total = 0
        self.max_queue_depth_normal = 0
        self._last_drop_log_ts = 0.0

        if normal_batch_interval_s is None and batch_interval_s is not None:
            normal_batch_interval_s = batch_interval_s
        if normal_max_batch is None and max_batch is not None:
            normal_max_batch = max_batch

        if normal_batch_interval_s is None:
            normal_batch_interval_s = EVENT_NORMAL_BATCH_INTERVAL_S
        self._normal_batch_interval_s = min(
            0.008, max(0.005, float(normal_batch_interval_s))
        )

        if normal_max_batch is None:
            normal_max_batch = EVENT_NORMAL_MAX_BATCH
        self._normal_max_batch = max(4, min(8, int(normal_max_batch)))

    def register_player(self, player: str) -> None:
        self._known_players.add(player)

    def unregister_player(self, player: str) -> None:
        self._known_players.discard(player)
        with self._lock:
            self._normal_queues.pop(player, None)
            timer = self._normal_timers.pop(player, None)
        if timer:
            timer.cancel()

    def set_active_player(self, player: str | None) -> None:
        if player is None:
            self._active_player = None
            return
        self.register_player(player)
        self._active_player = player

    @debounce("^(tap\\.|click\\.|next_round_click)$", window_ms=20)
    def route(self, event: UIEvent) -> None:
        targets = self._select_targets(event)
        if not targets:
            return
        flush_jobs: list[tuple[str, Sequence[UIEvent]]] = []
        high_jobs: list[tuple[str, UIEvent]] = []
        priority = self._resolve_priority(event)
        with self._lock:
            for target in targets:
                if priority is Priority.HIGH:
                    self.events_high_total += 1
                    metrics.inc("events_high_total")
                    high_jobs.append((target, event))
                    continue
                self.events_normal_total += 1
                metrics.inc("events_normal_total")
                queue = self._normal_queues.setdefault(target, deque())
                queue.append(event)
                queue_len = len(queue)
                if queue_len > self.max_queue_depth_normal:
                    self.max_queue_depth_normal = queue_len
                self._enforce_backpressure(target, queue)
                if len(queue) >= self._normal_max_batch:
                    batch = list(queue)
                    queue.clear()
                    timer = self._normal_timers.pop(target, None)
                    if timer:
                        timer.cancel()
                    flush_jobs.append((target, batch))
                    continue
                timer = self._normal_timers.get(target)
                if timer is None:
                    delay = max(0.0, self._normal_batch_interval_s)
                    timer = threading.Timer(delay, self._flush_normal_timer, args=(target,))
                    timer.daemon = True
                    self._normal_timers[target] = timer
                    timer.start()
        for target, job_event in high_jobs:
            self._deliver(target, job_event)
        for target, batch in flush_jobs:
            if batch:
                self.normal_batches_total += 1
                self._flush_batch(target, batch)

    def flush_all(self) -> None:
        with self._lock:
            items = list(self._normal_queues.items())
            self._normal_queues.clear()
            timers = list(self._normal_timers.values())
            self._normal_timers.clear()
        for timer in timers:
            timer.cancel()
        for target, queue in items:
            if queue:
                self.normal_batches_total += 1
                self._flush_batch(target, list(queue))

    # ------------------------------------------------------------------
    def _select_targets(self, event: UIEvent) -> Sequence[str]:
        if event.target:
            self.register_player(event.target)
            return (event.target,)
        if event.broadcast:
            if self._multi_route:
                return tuple(sorted(self._known_players))
            if self._active_player:
                return (self._active_player,)
            return ()
        if self._active_player:
            return (self._active_player,)
        return ()

    def _resolve_priority(self, event: UIEvent) -> Priority:
        priority = event.priority
        if isinstance(priority, Priority):
            return priority
        if isinstance(priority, str):
            lowered = priority.lower()
            if lowered == "high":
                return Priority.HIGH
            if lowered in {"normal", "low"}:
                return Priority.NORMAL
        return classify_event(event.name)

    def _enforce_backpressure(self, target: str, queue: Deque[UIEvent]) -> None:
        if len(queue) <= self._NORMAL_MAX_DEPTH:
            return
        dropped = 0
        while len(queue) > self._NORMAL_MAX_DEPTH:
            markers: list[UIEvent] = []
            dropped_candidate = None
            while queue:
                candidate = queue.popleft()
                if classify_event(candidate.name) is Priority.HIGH or candidate.name.startswith(
                    "marker."
                ):
                    markers.append(candidate)
                    continue
                dropped_candidate = candidate
                dropped += 1
                break
            for marker in reversed(markers):
                queue.appendleft(marker)
            if dropped_candidate is None:
                break
        if dropped:
            now = time.monotonic()
            if now - self._last_drop_log_ts >= 10.0:
                log.warning(
                    "Dropped %d normal-priority events for %s due to backpressure",
                    dropped,
                    target,
                )
                self._last_drop_log_ts = now

    def _flush_normal_timer(self, player: str) -> None:
        with self._lock:
            queue = self._normal_queues.get(player)
            if not queue:
                self._normal_timers.pop(player, None)
                return
            batch = list(queue)
            queue.clear()
            self._normal_timers.pop(player, None)
        if batch:
            self.normal_batches_total += 1
            self._flush_batch(player, batch)

    def _flush_batch(self, player: str, batch: Sequence[UIEvent]) -> None:
        for event in batch:
            self._deliver(player, event)
