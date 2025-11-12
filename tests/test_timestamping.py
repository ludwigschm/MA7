import json
from pathlib import Path

import pytest

from core.event_router import TimestampPolicy, policy_for
from core.time_sync import TimeSyncManager
from tabletop.pupil_bridge import PupilBridge


class _StubDevice:
    def __init__(self) -> None:
        self.events: list[str] = []

    def send_event(self, *args, **kwargs) -> None:
        if args:
            self.events.append(str(args[0]))


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> PupilBridge:
    monkeypatch.setattr("tabletop.pupil_bridge._reachable", lambda *_, **__: True)
    monkeypatch.setenv("LOW_LATENCY_DISABLED", "1")
    config_path = tmp_path / "devices.txt"
    config_path.write_text("VP1_IP=127.0.0.1\nVP1_PORT=8080\n", encoding="utf-8")
    bridge = PupilBridge(device_mapping={}, config_path=config_path)
    device = _StubDevice()
    bridge._device_by_player["VP1"] = device  # type: ignore[attr-defined]
    async def _measure(samples: int, timeout: float) -> list[float]:
        return [0.0]

    manager = TimeSyncManager("vp1", _measure)
    bridge._time_sync["VP1"] = manager  # type: ignore[attr-defined]
    bridge.ready.set()
    yield bridge
    bridge.close()


def test_policy_helper_arrival(monkeypatch: pytest.MonkeyPatch, bridge: PupilBridge) -> None:
    assert policy_for("sensor.gyro") is TimestampPolicy.ARRIVAL

    bridge.send_event("sensor.gyro", "VP1", {"value": 1})
    bridge._event_router.flush_all()  # type: ignore[attr-defined]

    device = bridge._device_by_player["VP1"]  # type: ignore[attr-defined]
    assert isinstance(device, _StubDevice)
    assert device.events
    record = device.events[-1]
    if "|" in record:
        name, encoded = record.split("|", 1)
        payload = json.loads(encoded)
    else:
        name = record
        payload = {}
    assert name == "sensor.gyro"
    assert "event_timestamp_unix_ns" not in payload


def test_ui_event_client_corrected_timestamp(
    monkeypatch: pytest.MonkeyPatch, bridge: PupilBridge
) -> None:
    assert policy_for("ui.test") is TimestampPolicy.CLIENT_CORRECTED

    offset_ns = 123_456_789
    ground_truth = 1_000_000_000 - offset_ns
    monkeypatch.setattr("time.monotonic_ns", lambda: 500_000_000)
    monkeypatch.setattr(
        "tabletop.pupil_bridge.host_unix_from_monotonic",
        lambda _: ground_truth + offset_ns,
    )
    monkeypatch.setattr(
        TimeSyncManager, "get_offset_ns", lambda self: offset_ns, raising=False
    )

    bridge.send_event("ui.test", "VP1", {})
    bridge._event_router.flush_all()  # type: ignore[attr-defined]

    device = bridge._device_by_player["VP1"]  # type: ignore[attr-defined]
    assert isinstance(device, _StubDevice)
    assert device.events
    name, encoded = device.events[-1].split("|", 1)
    assert name == "ui.test"
    payload = json.loads(encoded)
    assert "event_timestamp_unix_ns" in payload
    assert abs(payload["event_timestamp_unix_ns"] - ground_truth) <= 1_500_000
