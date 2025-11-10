import json
from pathlib import Path

import pytest

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
    yield bridge
    bridge.close()


def test_device_timestamp_included(monkeypatch: pytest.MonkeyPatch, bridge: PupilBridge) -> None:
    offset_ns = 123_456_789
    monkeypatch.setattr(
        TimeSyncManager, "get_offset_ns", lambda self: offset_ns, raising=False
    )
    monkeypatch.setattr(
        "tabletop.pupil_bridge.host_monotonic_to_unix_ns", lambda _: 1_000_000_000
    )
    monkeypatch.setattr("time.monotonic_ns", lambda: 500_000_000)

    bridge.send_event("ui.test", "VP1", {"value": 1})
    bridge._event_router.flush_all()  # type: ignore[attr-defined]

    device = bridge._device_by_player["VP1"]  # type: ignore[attr-defined]
    assert isinstance(device, _StubDevice)
    assert device.events
    name, encoded = device.events[-1].split("|", 1)
    assert name == "ui.test"
    payload = json.loads(encoded)
    assert payload["timestamp_ns"] == 1_000_000_000 - offset_ns
