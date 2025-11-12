"""Clock utilities for timestamping."""

import time


def now_ns() -> int:
    """Global timestamp (UNIX epoch, matches Companion Device)."""

    return time.time_ns()


def now_mono_ns() -> int:
    """Local monotonic clock for durations (never compare across devices)."""

    return time.monotonic_ns()

