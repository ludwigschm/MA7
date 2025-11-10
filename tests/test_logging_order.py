import os
from types import SimpleNamespace

os.environ.setdefault("KIVY_WINDOW", "mock")
os.environ.setdefault("KIVY_GRAPHICS", "mock")
os.environ.setdefault("KIVY_AUDIO", "mock")
os.environ.setdefault("KIVY_TEXT", "mock")

import pytest

pytest.importorskip("kivy")

from tabletop.state.controller import TabletopController, TabletopState
import tabletop.tabletop_view as tabletop_view
from tabletop.tabletop_view import TabletopRoot


@pytest.fixture
def tabletop_root(monkeypatch, mocker):
    monkeypatch.setattr(tabletop_view, "resolve_background_texture", lambda: None)
    state = TabletopState()
    controller = TabletopController(state)
    root = TabletopRoot(controller=controller, state=state)
    root._input_debouncer.allow = mocker.Mock(return_value=True)
    root._emit_button_bridge_event = mocker.Mock()
    root.update_user_displays = mocker.Mock()
    return root


def test_tap_card_logs_before_flip(mocker, tabletop_root):
    order = []
    widget = mocker.Mock()
    widget.flip.side_effect = lambda: order.append(("flip", None))
    tabletop_root.card_widget_for_player = mocker.Mock(return_value=widget)
    tabletop_root.record_action = mocker.Mock(
        side_effect=lambda *args, **kwargs: order.append(("record", None))
    )
    def _log_recorder(*_args, **kwargs):
        order.append(("log", kwargs.get("phase")))

    tabletop_root.log_event = mocker.Mock(side_effect=_log_recorder)

    result = SimpleNamespace(
        allowed=True,
        record_text="card flipped",
        log_action="reveal_inner",
        log_payload={"card": 1},
        next_phase=None,
    )
    tabletop_root.controller.tap_card = mocker.Mock(return_value=result)

    tabletop_root.tap_card(1, "inner")

    assert tabletop_root.log_event.call_count == 2
    assert widget.flip.call_count == 1
    assert order[:4] == [
        ("log", "input_received"),
        ("log", "action_applied"),
        ("flip", None),
        ("record", None),
    ]


def test_pick_signal_logs_before_button_update(mocker, tabletop_root):
    order = []
    button = mocker.Mock()
    button.set_pressed_state.side_effect = lambda: order.append(("pressed", None))
    tabletop_root.signal_buttons = {1: {"mittel": "btn_mid"}}
    tabletop_root.wid_safe = mocker.Mock(return_value=button)
    tabletop_root.record_action = mocker.Mock(
        side_effect=lambda *args, **kwargs: order.append(("record", None))
    )
    def _signal_log_recorder(*_args, **kwargs):
        order.append(("log", kwargs.get("phase")))

    tabletop_root.log_event = mocker.Mock(side_effect=_signal_log_recorder)

    result = SimpleNamespace(
        accepted=True,
        log_payload={"level": "mittel"},
        next_phase=None,
    )
    tabletop_root.controller.pick_signal = mocker.Mock(return_value=result)

    tabletop_root.pick_signal(1, "mittel")

    assert tabletop_root.log_event.call_count == 2
    assert button.set_pressed_state.call_count == 1
    assert order[:4] == [
        ("log", "input_received"),
        ("log", "action_applied"),
        ("pressed", None),
        ("record", None),
    ]

