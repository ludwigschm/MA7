"""Simple CLI to inspect host/device time synchronisation jitter."""

from __future__ import annotations

import math
import statistics
import time
from typing import List

from core.time_sync import TimeSyncManager, get_state

DURATION_S = 120
_INTERVAL_S = 1.0


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return float("nan")
    sorted_values = sorted(values)
    if percentile <= 0:
        return sorted_values[0]
    if percentile >= 1:
        return sorted_values[-1]
    index = (len(sorted_values) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    weight = index - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def main() -> int:
    offsets_ms: list[float] = []
    rms_samples_ms: list[float] = []
    deadline = time.monotonic() + DURATION_S
    while time.monotonic() < deadline:
        state = get_state()
        offsets_ms.append(state.median_ns / 1_000_000.0)
        rms_samples_ms.append(state.rms_ns / 1_000_000.0)
        time.sleep(_INTERVAL_S)

    if not offsets_ms:
        print("No samples collected; ensure a TimeSyncManager is running.")
        return 1

    median_offset = statistics.median(offsets_ms)
    rms_jitter = math.sqrt(
        sum((value - median_offset) ** 2 for value in offsets_ms) / len(offsets_ms)
    )
    p95_offset = _percentile([abs(value) for value in offsets_ms], 0.95)
    avg_internal_rms = math.sqrt(
        sum(sample ** 2 for sample in rms_samples_ms) / len(rms_samples_ms)
    )

    print(f"Median offset: {median_offset:.3f} ms")
    print(f"RMS jitter: {rms_jitter:.3f} ms")
    print(f"P95 offset: {p95_offset:.3f} ms")
    print(f"Avg internal RMS: {avg_internal_rms:.3f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
