import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tabletop.logging.cloud_filter import classify_cloud_logging


@pytest.fixture
def log_event_context(monkeypatch):
    os.environ.setdefault("KIVY_WINDOW", "mock")
    os.environ.setdefault("KIVY_GRAPHICS", "mock")
    os.environ.setdefault("KIVY_AUDIO", "mock")
    os.environ.setdefault("KIVY_TEXT", "mock")

    try:
        import tabletop.tabletop_view as tabletop_view
        from tabletop.tabletop_view import TabletopRoot
    except ModuleNotFoundError as exc:
        pytest.skip("Kivy not installed")
    except SystemExit as exc:
        pytest.skip("Kivy text provider unavailable")

    monkeypatch.setattr(tabletop_view, "write_round_log", lambda *args, **kwargs: None)
    push_mock = Mock()
    monkeypatch.setattr(tabletop_view, "push_async", push_mock)

    root = SimpleNamespace(
        logger=Mock(),
        session_configured=True,
        round=1,
        session_id="session-1",
        player_roles={},
        marker_bridge=None,
    )
    root.phase = "ux"
    root.logger.log_event.return_value = object()
    root._actor_label = lambda player: "SYS" if player is None else f"P{player}"
    root.current_engine_phase = lambda: "engine"
    root._bridge_payload_base = Mock(return_value={"session": 1, "block": 2})

    return SimpleNamespace(
        TabletopRoot=TabletopRoot,
        root=root,
        push_async=push_mock,
    )


def test_classify_cloud_logging_accepts_button_action_applied():
    payload = {"phase": "action_applied", "button": "start"}
    assert classify_cloud_logging("start_click", payload) == (True, True)


def test_classify_cloud_logging_rejects_input_phase():
    payload = {"phase": "input_received", "button": "start"}
    assert classify_cloud_logging("start_click", payload) == (False, False)


def test_classify_cloud_logging_accepts_round_start():
    payload = {"phase": "action_applied"}
    assert classify_cloud_logging("round_start", payload) == (True, False)


def test_classify_cloud_logging_rejects_non_button_action():
    payload = {"phase": "action_applied"}
    assert classify_cloud_logging("showdown", payload) == (False, False)


def test_log_event_skips_payload_generation_when_filtered(log_event_context):
    ctx = log_event_context
    ctx.TabletopRoot.log_event(ctx.root, None, "showdown", {"phase": "action_applied"})

    ctx.root._bridge_payload_base.assert_not_called()
    ctx.push_async.assert_not_called()


def test_log_event_builds_payload_for_button_action(log_event_context):
    ctx = log_event_context
    ctx.root.marker_bridge = SimpleNamespace(enqueue=Mock())
    ctx.root.player_roles = {1: 2}

    ctx.TabletopRoot.log_event(ctx.root, 1, "button_press", {"button": "start"})

    ctx.root._bridge_payload_base.assert_called_once_with(player=None)
    ctx.push_async.assert_called_once()
    ctx.root.marker_bridge.enqueue.assert_called_once()
