import asyncio

import pytest

from core.recording import RecordingController, RecordingHttpError, recording_session


class _IdempotentClient:
    def __init__(self) -> None:
        self.started = False
        self.start_attempts = 0
        self.cancel_attempts = 0

    async def is_recording(self) -> bool:
        return self.started

    async def recording_start(self, *, label: str | None = None) -> None:
        self.start_attempts += 1
        raise RecordingHttpError(400, "Already recording!")

    async def recording_begin(self) -> dict[str, str]:
        return {"recording_id": "existing"}

    async def recording_stop(self) -> None:
        self.started = False

    async def recording_cancel(self) -> None:
        self.cancel_attempts += 1
        self.started = False


class _TransientClient:
    def __init__(self) -> None:
        self.started = False
        self.start_attempts = 0
        self.cancel_attempts = 0

    async def is_recording(self) -> bool:
        return self.started

    async def recording_start(self, *, label: str | None = None) -> None:
        self.start_attempts += 1
        if self.start_attempts < 2:
            raise RecordingHttpError(503, "temporary error", transient=True)
        self.started = True

    async def recording_begin(self) -> dict[str, str]:
        return {"recording_id": "fresh"}

    async def recording_stop(self) -> None:
        self.started = False

    async def recording_cancel(self) -> None:
        self.cancel_attempts += 1
        self.started = False


class _TimeoutClient(_TransientClient):
    async def recording_begin(self) -> None:
        raise asyncio.TimeoutError


class _SessionBridge:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def recording_start(self, player: str, *, label: str) -> str:
        self.events.append(f"start:{player}:{label}")
        return "rid-1"

    async def recording_begin(self, player: str) -> dict[str, str]:
        self.events.append(f"begin:{player}")
        return {"recording_id": "rid-1"}

    async def recording_stop_and_save(self, player: str) -> None:
        self.events.append(f"stop:{player}")


def test_ensure_started_idempotent_on_400_already_recording():
    client = _IdempotentClient()
    controller = RecordingController(client)
    asyncio.run(controller.ensure_started(label="test"))
    assert client.start_attempts == 1
    assert asyncio.run(controller.is_recording()) is True


def test_ensure_started_transient_retry_then_success(monkeypatch: pytest.MonkeyPatch):
    client = _TransientClient()
    controller = RecordingController(client)

    async def fast_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    asyncio.run(controller.ensure_started(label="retry"))
    assert client.start_attempts == 2
    assert asyncio.run(controller.is_recording()) is True


def test_begin_segment_timeout_best_effort(caplog):
    client = _TimeoutClient()
    controller = RecordingController(client)
    client.started = True
    controller._active = True  # type: ignore[attr-defined]
    caplog.set_level("WARNING")
    asyncio.run(controller.begin_segment(deadline_ms=10))
    assert any("best-effort" in record.message for record in caplog.records)


def test_recording_session_awaits_begin_and_stops():
    bridge = _SessionBridge()

    async def runner() -> None:
        async with recording_session(bridge, "VP1", "label-1") as rid:
            assert rid == "rid-1"
            assert bridge.events == ["start:VP1:label-1", "begin:VP1"]

    asyncio.run(runner())
    assert bridge.events == ["start:VP1:label-1", "begin:VP1", "stop:VP1"]


def test_recording_session_stops_on_exception():
    bridge = _SessionBridge()

    async def runner() -> None:
        async with recording_session(bridge, "VP2", "oops"):
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        asyncio.run(runner())

    assert bridge.events[-1] == "stop:VP2"


def test_cancel_resets_controller_state():
    client = _TransientClient()
    controller = RecordingController(client)
    client.started = True
    controller._active = True  # type: ignore[attr-defined]

    asyncio.run(controller.cancel())

    assert controller._active is False  # type: ignore[attr-defined]
    assert client.cancel_attempts == 1
