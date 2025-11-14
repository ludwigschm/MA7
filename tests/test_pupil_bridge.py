import json
import math
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator, Tuple

import pytest

from tabletop.pupil_bridge import (
    NeonDeviceConfig,
    PupilBridge,
    device_key_from,
)


class _FakeEstimate:
    def __init__(self, offset_ms: float, rtt_ms: float = 10.0) -> None:
        self.time_offset_ms = SimpleNamespace(mean=offset_ms)
        self.roundtrip_duration_ms = SimpleNamespace(mean=rtt_ms)


class _FakeDevice:
    def __init__(self) -> None:
        self.events: list[Tuple[Tuple[object, ...], dict]] = []
        self.start_calls = 0
        self.cancel_calls = 0
        self.active = False
        self.offset_index = 0
        self.offset_samples_ms = [10.0 + i * 0.5 for i in range(20)]
        self._notifications: dict[str, object] = {"recording.begin": {"recording_id": "fake"}}

    def recording_start(self) -> None:
        self.start_calls += 1
        self.active = True

    def recording_stop(self) -> None:  # pragma: no cover - defensive
        self.active = False

    def recording_cancel(self) -> None:
        self.cancel_calls += 1
        self._notifications["recording.cancelled"] = {"recording_id": "fake"}
        self.active = False

    def wait_for_notification(self, event: str, timeout: float = 0.5) -> object:
        return self._notifications.get(event)

    def estimate_time_offset(self) -> _FakeEstimate:
        value = self.offset_samples_ms[
            self.offset_index % len(self.offset_samples_ms)
        ]
        self.offset_index += 1
        return _FakeEstimate(value)

    def send_event(self, *args, **kwargs) -> None:
        self.events.append((args, kwargs))


def _decoded_events(device: _FakeDevice) -> Iterator[tuple[str, dict]]:
    for args, kwargs in device.events:
        if "name" in kwargs:
            name = str(kwargs.get("name"))
            payload = dict(kwargs.get("payload", {}) or {})
            yield name, payload
            continue
        if not args:
            continue
        first = args[0]
        if not isinstance(first, str):
            continue
        if "|" in first:
            name, encoded = first.split("|", 1)
            try:
                payload = json.loads(encoded)
            except json.JSONDecodeError:
                payload = {}
            yield name, payload
        else:
            yield first, {}


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch) -> Tuple[PupilBridge, _FakeDevice]:
    monkeypatch.setattr("tabletop.pupil_bridge._reachable", lambda *_, **__: True)
    monkeypatch.setenv("LOW_LATENCY_DISABLED", "1")
    config_path = Path("/tmp/test_neon_devices.txt")
    config_path.write_text("VP1_IP=127.0.0.1\nVP1_PORT=8080\n", encoding="utf-8")
    bridge = PupilBridge(device_mapping={}, config_path=config_path)
    device = _FakeDevice()
    cfg = NeonDeviceConfig(player="VP1", ip="127.0.0.1", port=8080)
    bridge._device_by_player["VP1"] = device  # type: ignore[attr-defined]
    device_key = device_key_from(cfg.ip, cfg.port or 0, None)
    bridge._on_device_connected("VP1", device, cfg, device_key)  # type: ignore[attr-defined]
    bridge.calibrate_time_offset(players=["VP1"])
    bridge.ready.set()
    yield bridge, device
    bridge.close()
    config_path.unlink(missing_ok=True)


def test_event_router_single_target(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.send_event("ui.test", "VP1", {"value": 42})
    pupil_bridge._event_router.flush_all()  # type: ignore[attr-defined]
    decoded = list(_decoded_events(device))
    assert decoded
    name, payload = decoded[0]
    assert name == "ui.test"
    assert "event_timestamp_unix_ns" in payload


def test_recording_start_idempotent(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.start_recording(1, 1, "VP1")
    pupil_bridge.start_recording(1, 1, "VP1")
    assert device.start_calls == 1
    assert pupil_bridge._recording_metadata["VP1"]["recording_id"] == "fake"  # type: ignore[index]
    pupil_bridge._event_router.flush_all()  # type: ignore[attr-defined]
    started_event = next(
        payload
        for name, payload in _decoded_events(device)
        if name == "session.recording_started"
    )
    assert started_event["recording_id"] == "fake"


def test_recording_label_update_without_restart(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.start_recording(1, 1, "VP1")
    first_label_event = next(
        payload
        for name, payload in _decoded_events(device)
        if name == "recording.label"
    )
    assert first_label_event["block"] == 1

    pupil_bridge.start_recording(1, 2, "VP1")
    assert device.start_calls == 1

    updated_label_event = next(
        payload
        for name, payload in reversed(list(_decoded_events(device)))
        if name == "recording.label"
    )
    assert updated_label_event["block"] == 2
    metadata = pupil_bridge._recording_metadata["VP1"]
    assert metadata["block"] == 2
    assert metadata["recording_id"] == "fake"


def test_recording_cancel_clears_state(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.start_recording(1, 1, "VP1")
    assert pupil_bridge.is_recording("VP1") is True

    pupil_bridge.recording_cancel("VP1")
    assert pupil_bridge.is_recording("VP1") is False
    assert "VP1" not in pupil_bridge._recording_metadata
    assert device.cancel_calls == 1

    pupil_bridge.recording_cancel("VP1")  # idempotent
    assert device.cancel_calls == 2

    pupil_bridge.start_recording(1, 1, "VP1")
    assert device.start_calls == 2
    assert pupil_bridge.is_recording("VP1") is True


def test_estimate_time_offset_returns_cached_value(bridge):
    pupil_bridge, device = bridge
    assert pupil_bridge._player_device_key["VP1"] == "127.0.0.1:8080"
    offset = pupil_bridge.estimate_time_offset("VP1")
    expected = device.offset_samples_ms[0] / 1000.0
    assert offset == pytest.approx(expected)
    # subsequent call should not error even if samples exhausted
    offset2 = pupil_bridge.estimate_time_offset("VP1")
    assert math.isclose(offset2, offset, rel_tol=1e-6)


def test_calibrate_time_offset_hard_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BrokenDevice(_FakeDevice):
        def estimate_time_offset(self) -> _FakeEstimate:  # type: ignore[override]
            raise RuntimeError("boom")

    monkeypatch.setattr("tabletop.pupil_bridge._reachable", lambda *_, **__: True)
    monkeypatch.setenv("LOW_LATENCY_DISABLED", "1")
    config_path = Path("/tmp/test_neon_devices_fail.txt")
    config_path.write_text("VP1_IP=127.0.0.1\nVP1_PORT=8080\n", encoding="utf-8")
    bridge = PupilBridge(device_mapping={}, config_path=config_path)
    try:
        device = _BrokenDevice()
        cfg = NeonDeviceConfig(player="VP1", ip="127.0.0.1", port=8080)
        bridge._device_by_player["VP1"] = device  # type: ignore[attr-defined]
        device_key = device_key_from(cfg.ip, cfg.port or 0, None)
        bridge._player_device_key["VP1"] = device_key  # type: ignore[attr-defined]
        with pytest.raises(RuntimeError):
            bridge.calibrate_time_offset(players=["VP1"])
    finally:
        bridge.close()
        config_path.unlink(missing_ok=True)
