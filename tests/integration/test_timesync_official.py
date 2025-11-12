import asyncio
import logging
import statistics
from types import SimpleNamespace
from typing import Callable, Iterable, List, Tuple

import pytest

from core.time_sync import OfficialTimeSync


class _FakeDevice:
    def __init__(self, samples: Iterable[Tuple[float, float]]) -> None:
        self._samples: List[Tuple[float, float]] = list(samples)
        if not self._samples:
            raise ValueError("samples must not be empty")
        self._index = 0

    async def estimate_time_offset(self) -> SimpleNamespace:
        await asyncio.sleep(0)
        if self._index < len(self._samples):
            offset_ms, rtt_ms = self._samples[self._index]
        else:
            offset_ms, rtt_ms = self._samples[-1]
        self._index += 1
        return SimpleNamespace(
            time_offset_ms=SimpleNamespace(mean=offset_ms),
            roundtrip_duration_ms=SimpleNamespace(mean=rtt_ms),
        )


@pytest.fixture
def device_factory() -> Callable[[Iterable[Tuple[float, float]]], _FakeDevice]:
    def _build(samples: Iterable[Tuple[float, float]]) -> _FakeDevice:
        return _FakeDevice(samples)

    return _build


def test_success_path(
    device_factory, caplog: pytest.LogCaptureFixture
) -> None:
    offsets = [(float(i), 10.0) for i in range(20)]
    device = device_factory(offsets)
    sync = OfficialTimeSync(
        device,
        "dev",
        min_samples=10,
        max_samples=20,
        inter_sample_sleep_s=0.0,
    )
    caplog.set_level(logging.INFO)
    result = asyncio.run(sync.calibrate())
    assert result.accepted == 20
    expected_median = statistics.median(o for o, _ in offsets)
    assert result.median_offset_ms == pytest.approx(expected_median)
    assert any("TIMESYNC_SUMMARY device=dev" in record.message for record in caplog.records)
    assert any("accepted=20/20" in record.message for record in caplog.records)


def test_reject_then_accept(device_factory) -> None:
    samples = [(50.0, 200.0) for _ in range(5)]
    samples += [(float(i), 20.0) for i in range(15)]
    device = device_factory(samples)
    sync = OfficialTimeSync(
        device,
        "dev",
        min_samples=10,
        max_samples=20,
        inter_sample_sleep_s=0.0,
    )
    result = asyncio.run(sync.calibrate())
    assert result.total == 20
    assert result.accepted == 15
    assert result.accepted >= sync.min_samples


def test_no_valid_samples(
    device_factory, caplog: pytest.LogCaptureFixture
) -> None:
    samples = [(500.0, 200.0) for _ in range(20)]
    device = device_factory(samples)
    sync = OfficialTimeSync(
        device,
        "dev",
        min_samples=5,
        max_samples=20,
        inter_sample_sleep_s=0.0,
    )
    caplog.set_level(logging.WARNING)
    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(sync.calibrate())
    assert "no valid samples" in str(excinfo.value)
    assert any(
        "TIMESYNC_SAMPLE device=dev" in record.message and "reject" in record.message
        for record in caplog.records
    )
