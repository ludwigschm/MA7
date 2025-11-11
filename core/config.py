"""Centralised configuration with environment overrides."""

from __future__ import annotations

import logging
import os
from typing import Callable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T", float, int)


def _get_env(name: str, default: T, caster: Callable[[str], T]) -> T:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return caster(value)
    except (TypeError, ValueError):
        log.warning("Invalid value for %s: %r (using default %r)", name, value, default)
        return default


EVENT_NORMAL_BATCH_INTERVAL_S: float = _get_env(
    "EVENT_NORMAL_BATCH_INTERVAL_S", 0.006, float
)
EVENT_NORMAL_MAX_BATCH: int = _get_env("EVENT_NORMAL_MAX_BATCH", 6, int)

TIMESYNC_RESYNC_INTERVAL_S: float = _get_env("TIMESYNC_RESYNC_INTERVAL_S", 30.0, float)
TIMESYNC_DRIFT_THRESHOLD_MS: float = _get_env("TIMESYNC_DRIFT_THRESHOLD_MS", 2.0, float)

HTTP_MAX_CONNECTIONS: int = _get_env("HTTP_MAX_CONNECTIONS", 8, int)
HTTP_CONNECT_TIMEOUT_S: float = _get_env("HTTP_CONNECT_TIMEOUT_S", 0.5, float)

