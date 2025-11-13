"""Kivy application bootstrap for the tabletop UI."""

from __future__ import annotations

import argparse
import os
import math
import statistics
import threading
import time
from collections import deque
from contextlib import suppress
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from queue import Queue
from typing import Any, Optional, Sequence, cast

import logging

from kivy.app import App
from kivy.config import Config

Config.set("graphics", "multisamples", "0")
Config.set("graphics", "maxfps", "60")
Config.set("graphics", "vsync", "1")
Config.set("kivy", "exit_on_escape", "0")
Config.write()

from kivy.clock import Clock
from kivy.core.window import Window
from kivy.lang import Builder
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.popup import Popup

from tabletop.data.config import ARUCO_OVERLAY_PATH
from tabletop.logging.round_csv import close_round_log, flush_round_log
from tabletop.logging.events_bridge import init_client as init_pupylabs_client
from tabletop.overlay.process import (
    OverlayProcess,
    start_overlay,
    stop_overlay,
)
from tabletop.tabletop_view import TabletopRoot
from tabletop.pupil_bridge import PupilBridge
from tabletop.utils.runtime import (
    is_low_latency_disabled,
    is_perf_logging_enabled,
)

log = logging.getLogger(__name__)

_KV_LOADED = False

_pupylabs_timeout = os.environ.get("PUPYLABS_TIMEOUT_S", "2.0")
_pupylabs_retries = os.environ.get("PUPYLABS_MAX_RETRIES", "3")
try:
    _timeout_value = float(_pupylabs_timeout)
except (TypeError, ValueError):
    _timeout_value = 2.0
try:
    _retry_value = int(_pupylabs_retries)
except (TypeError, ValueError):
    _retry_value = 3

init_pupylabs_client(
    base_url=os.environ.get("PUPYLABS_BASE_URL", "https://cloud.pupylabs.example"),
    api_key=os.environ.get("PUPYLABS_API_KEY", ""),
    timeout_s=_timeout_value,
    max_retries=_retry_value,
)


class TabletopApp(App):
    """Main Kivy application that wires the UI with infrastructure services."""

    def __init__(
        self,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
        player: str = "auto",
        players: Optional[Sequence[str]] = None,
        bridge: Optional[PupilBridge] = None,
        single_block_mode: bool = False,
        logging_queue: Optional[Queue] = None,
        bridge_error: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self._overlay_process: Optional[OverlayProcess] = None
        self._esc_handler: Optional[Any] = None
        self._key_up_handler: Optional[Any] = None
        self._bootstrap_screens: list[dict[str, int]] = self._probe_screens_pyqt()
        self._target_display_index: int = self._determine_display_index(
            screens=self._bootstrap_screens
        )

        self._configure_startup_display(self._target_display_index)
        self._bridge: Optional[PupilBridge] = bridge
        self._session: Optional[int] = session
        self._block: Optional[int] = block
        requested_players: set[str] = set()
        if players is not None:
            requested_players.update(players)
        elif player:
            lowered = player.lower()
            if lowered == "both":
                requested_players.update({"VP1", "VP2"})
            elif lowered not in {"auto"}:
                requested_players.add(player)
        self._players: set[str] = {entry for entry in requested_players if entry}
        self._single_block_mode: bool = single_block_mode
        self._perf_logging: bool = is_perf_logging_enabled()
        self._low_latency_disabled: bool = is_low_latency_disabled()
        self._logging_queue: Optional[Queue] = logging_queue
        self._logging_queue_maxsize: int = (
            logging_queue.maxsize if logging_queue is not None else 0
        )
        self._frame_samples = deque(maxlen=600)
        self._frame_sampler = None
        self._frame_log_event = None
        self._queue_monitor_event = None
        self._last_queue_warning = 0.0
        self._bridge_connect_error_reason: Optional[str] = bridge_error
        self._bridge_retry_popup: Optional[Popup] = None
        super().__init__(**kwargs)

    @staticmethod
    def _describe_window_screens() -> list[dict[str, int]]:
        """Return available screen geometries from the active Kivy window."""

        screens = getattr(Window, "screens", None)
        described: list[dict[str, int]] = []
        if not screens:
            return described

        for screen in screens:
            entry = {"left": 0, "top": 0, "width": 0, "height": 0}

            pos = getattr(screen, "pos", None)
            if pos is not None:
                with suppress(Exception):
                    entry["left"], entry["top"] = (int(pos[0]), int(pos[1]))
            else:
                entry["left"] = int(getattr(screen, "x", 0))
                entry["top"] = int(getattr(screen, "y", 0))

            size = getattr(screen, "size", None)
            if size is not None:
                with suppress(Exception):
                    entry["width"], entry["height"] = (
                        int(size[0]),
                        int(size[1]),
                    )
            else:
                entry["width"] = int(getattr(screen, "width", Window.width))
                entry["height"] = int(getattr(screen, "height", Window.height))

            described.append(entry)

        return described

    @staticmethod
    def _probe_screens_pyqt() -> list[dict[str, int]]:
        """Probe system displays via PyQt as a fallback during bootstrap."""

        try:
            from PyQt6.QtGui import QGuiApplication
        except Exception:  # pragma: no cover - optional dependency
            return []

        app = QGuiApplication.instance()
        owns_app = False
        if app is None:
            try:
                app = QGuiApplication([])
                owns_app = True
            except Exception:  # pragma: no cover - optional dependency
                return []

        screens: list[dict[str, int]] = []
        try:
            for screen in app.screens():
                try:
                    geometry = screen.geometry()
                except Exception:  # pragma: no cover - defensive fallback
                    continue
                screens.append(
                    {
                        "left": int(geometry.x()),
                        "top": int(geometry.y()),
                        "width": int(geometry.width()),
                        "height": int(geometry.height()),
                    }
                )
        finally:
            if owns_app:
                app.quit()

        return screens

    @staticmethod
    def _clamp_display_index(
        display_index: int, *, screens: Optional[Sequence[dict[str, int]]] = None
    ) -> int:
        """Clamp the desired display index to the available displays."""

        if display_index < 0:
            return 0

        if screens is None:
            screens = TabletopApp._describe_window_screens()
            if not screens:
                screens = None

        if screens:
            return min(display_index, len(screens) - 1)

        return display_index

    def _determine_display_index(
        self, *, screens: Optional[Sequence[dict[str, int]]] = None
    ) -> int:
        """Choose the preferred display for the experiment window."""

        env_value = os.environ.get("TABLETOP_DISPLAY_INDEX")
        desired_index: Optional[int] = None

        if env_value is not None:
            try:
                desired_index = int(env_value)
            except ValueError:
                log.warning(
                    "Ignoring invalid TABLETOP_DISPLAY_INDEX=%r", env_value
                )

        if desired_index is None:
            if screens is None:
                screens = TabletopApp._describe_window_screens()
                if not screens:
                    screens = self._bootstrap_screens
            count = len(screens) if screens is not None else 0
            desired_index = 1 if count >= 2 else 0

        return self._clamp_display_index(desired_index, screens=screens)

    def _apply_display_environment(self, display_index: int) -> None:
        """Persist the chosen display index for child processes."""

        os.environ["TABLETOP_DISPLAY_INDEX"] = str(display_index)
        os.environ["SDL_VIDEO_FULLSCREEN_DISPLAY"] = str(display_index)

    def _configure_startup_display(self, display_index: int) -> None:
        """Prepare environment and Kivy configuration for the selected monitor."""

        self._apply_display_environment(display_index)

        with suppress(Exception):
            Config.set("graphics", "display", str(display_index))

        target_screen: Optional[dict[str, int]] = None
        if 0 <= display_index < len(self._bootstrap_screens):
            target_screen = self._bootstrap_screens[display_index]

        if target_screen:
            with suppress(Exception):
                Config.set("graphics", "position", "custom")
                Config.set("graphics", "left", str(target_screen["left"]))
                Config.set("graphics", "top", str(target_screen["top"]))
                Config.set("graphics", "width", str(target_screen["width"]))
                Config.set("graphics", "height", str(target_screen["height"]))
            log.info(
                "Bootstrap configured for display %s at (%s, %s) size (%s x %s)",
                display_index,
                target_screen["left"],
                target_screen["top"],
                target_screen["width"],
                target_screen["height"],
            )

        with suppress(Exception):
            Config.write()

    def _move_window_to_display(self, display_index: int) -> int:
        """Attempt to position the window on the requested display."""

        screens = TabletopApp._describe_window_screens()
        if screens:
            self._bootstrap_screens = list(screens)
        else:
            screens = self._bootstrap_screens

        if not screens:
            return display_index

        clamped = self._clamp_display_index(display_index, screens=screens)
        try:
            target = screens[clamped]
        except Exception:  # pragma: no cover - defensive fallback
            log.exception("Failed to access display information for index %s", clamped)
            return clamped

        try:
            left = int(target.get("left", getattr(Window, "left", 0)))
            top = int(target.get("top", getattr(Window, "top", 0)))
            width = int(target.get("width", Window.width))
            height = int(target.get("height", Window.height))

            with suppress(Exception):
                Window.position = "custom"
            Window.left = left
            Window.top = top
            Window.size = (width, height)
            log.info(
                "Window moved to display %s at (%s, %s) size (%s x %s)",
                clamped,
                left,
                top,
                width,
                height,
            )
        except Exception:  # pragma: no cover - defensive fallback
            log.exception("Failed to reposition window for display %s", clamped)

        return clamped

    def build(self) -> TabletopRoot:
        """Create the root widget for the Kivy application."""
        global _KV_LOADED
        if not _KV_LOADED:
            kv_path = Path(__file__).parent / "ui" / "layout.kv"
            if kv_path.exists():
                Builder.load_file(str(kv_path))
            _KV_LOADED = True

        primary_player = next(iter(self._players), "")
        root = TabletopRoot(
            bridge=self._bridge,
            bridge_player=primary_player,
            bridge_session=self._session,
            bridge_block=self._block,
            single_block_mode=self._single_block_mode,
            perf_logging=self._perf_logging,
        )
        # propagate multi-player context so the view can start/stop recordings for all
        try:
            root.update_bridge_context(
                bridge=self._bridge,
                players=set(self._players),
                session=self._session,
                block=self._block,
            )
        except Exception:
            pass

        # ESC binding is scheduled in ``on_start`` once the window exists.
        return root

    # ------------------------------------------------------------------
    # Bridge helpers
    def _bridge_payload_base(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self._session is not None:
            payload["session"] = self._session
        if self._block is not None:
            payload["block"] = self._block
        return payload

    def _iter_active_players(self) -> list[str]:
        if not self._bridge:
            return []

        players = set(self._players)
        if not players:
            try:
                players = set(self._bridge.connected_players())
            except AttributeError:
                players = set()
            if players:
                self._players = set(players)

        return [player for player in players if self._bridge.is_connected(player)]

    def _format_key_name(self, key: int, codepoint: str) -> str:
        if codepoint:
            if codepoint == " ":
                return "space"
            return codepoint
        return f"code_{key}"

    def _emit_bridge_key_event(
        self,
        action: str,
        *,
        key: int,
        scancode: int,
        codepoint: str,
        modifiers: list[str],
    ) -> None:
        if not self._bridge:
            return
        key_name = self._format_key_name(key, codepoint)
        event_name = f"key.{key_name}.{action}"
        payload = self._bridge_payload_base()
        payload.update(
            {
                "key": key_name,
                "keycode": key,
                "scancode": scancode,
                "codepoint": codepoint,
                "modifiers": modifiers,
            }
        )
        root = cast(Optional[TabletopRoot], self.root)
        if root is not None:
            try:
                phase_value = getattr(root, "phase", None)
                if phase_value is not None:
                    payload.setdefault(
                        "phase",
                        getattr(phase_value, "name", str(phase_value)),
                    )
            except Exception:
                pass
            try:
                round_value = getattr(root, "round", None)
                if isinstance(round_value, int):
                    payload.setdefault("round_index", max(0, round_value - 1))
            except Exception:
                pass
            marker_bridge = getattr(root, "marker_bridge", None)
            if marker_bridge:
                marker_bridge.enqueue(event_name, payload)  # enriched payload (non-blocking)
            else:
                root.send_bridge_event(event_name, payload)
            return

        players = self._iter_active_players()
        if not players:
            return
        for player in players:
            payload_copy = dict(payload)
            payload_copy["target_player"] = player
            self._bridge.send_event(event_name, player, payload_copy)

    def _bind_esc(self) -> None:
        """Ensure ESC toggles fullscreen without closing the app."""

        if self._esc_handler is not None:
            return

        def _on_key_down(
            _window: Window,
            key: int,
            scancode: int,
            codepoint: str,
            modifiers: list[str],
        ) -> bool:
            try:
                self._emit_bridge_key_event(
                    "down",
                    key=key,
                    scancode=scancode,
                    codepoint=codepoint,
                    modifiers=modifiers,
                )
            except Exception:  # pragma: no cover - defensive fallback
                log.exception("Failed to emit bridge key down event")
            if key == 27:  # ESC
                try:
                    if Window.fullscreen:
                        Window.fullscreen = False
                        Window.borderless = False
                    else:
                        Window.fullscreen = "auto"
                        Window.borderless = True
                    log.info("ESC toggled fullscreen. Now fullscreen=%s", Window.fullscreen)
                except Exception as exc:  # pragma: no cover - safety net
                    log.exception("Error toggling fullscreen: %s", exc)
                return True
            return False

        self._esc_handler = _on_key_down
        Window.bind(on_key_down=self._esc_handler)

        if self._key_up_handler is not None:
            return

        def _on_key_up(
            _window: Window,
            key: int,
            scancode: int,
            *args: Any,
        ) -> bool:
            try:
                self._emit_bridge_key_event(
                    "up",
                    key=key,
                    scancode=scancode,
                    codepoint="",
                    modifiers=list(args[0]) if args and isinstance(args[0], (list, tuple)) else [],
                )
            except Exception:  # pragma: no cover - defensive fallback
                log.exception("Failed to emit bridge key up event")
            return False

        self._key_up_handler = _on_key_up
        Window.bind(on_key_up=self._key_up_handler)

    # ------------------------------------------------------------------
    # Performance instrumentation
    def _track_frame_time(self, dt: float) -> None:
        """Track frame durations for percentile logging."""

        if not self._perf_logging:
            return
        self._frame_samples.append(dt * 1000.0)

    def _percentile(self, data: list[float], fraction: float) -> float:
        if not data:
            return 0.0
        if fraction <= 0:
            return data[0]
        if fraction >= 1:
            return data[-1]
        position = (len(data) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return data[int(position)]
        lower_val = data[lower]
        upper_val = data[upper]
        return lower_val + (upper_val - lower_val) * (position - lower)

    def _log_frame_metrics(self, _dt: float) -> None:
        if not self._perf_logging or not self._frame_samples:
            return
        samples = sorted(self._frame_samples)
        p50 = self._percentile(samples, 0.50)
        p95 = self._percentile(samples, 0.95)
        p99 = self._percentile(samples, 0.99)
        log.info(
            "Frame timing percentiles (ms): p50=%.2f p95=%.2f p99=%.2f", p50, p95, p99
        )

    def _monitor_queues(self, _dt: float) -> None:
        if not self._perf_logging:
            return
        now = time.monotonic()
        if self._logging_queue is not None and self._logging_queue_maxsize > 0:
            load = self._logging_queue.qsize() / self._logging_queue_maxsize
            if load >= 0.8 and now - self._last_queue_warning >= 1.0:
                log.warning("Logging queue at %.0f%% capacity", load * 100.0)
                self._last_queue_warning = now
        bridge = self._bridge
        if bridge is not None:
            size, capacity = bridge.event_queue_load()
            if capacity > 0:
                load = size / capacity
                if load >= 0.8 and now - self._last_queue_warning >= 1.0:
                    log.warning("Pupil event queue at %.0f%% capacity", load * 100.0)
                    self._last_queue_warning = now

    def _cancel_event(self, event: Any) -> None:
        if event is None:
            return
        cancel = getattr(event, "cancel", None)
        if callable(cancel):
            cancel()

    def _show_bridge_error_dialog(self, reason: str) -> None:
        if self._bridge_retry_popup is not None:
            try:
                self._bridge_retry_popup.dismiss()
            except Exception:
                pass
            self._bridge_retry_popup = None

        content = BoxLayout(orientation="vertical", spacing=16, padding=24)
        message_label = Label(
            text="Verbindung fehlgeschlagen. Prüfe Netzwerk/Companion.\nErneut versuchen?",
            halign="center",
            valign="middle",
            size_hint_y=None,
        )
        message_label.bind(
            texture_size=lambda inst, _value: setattr(inst, "height", inst.texture_size[1] + 10),
            width=lambda inst, value: setattr(inst, "text_size", (value, None)),
        )
        content.add_widget(message_label)

        reason = (reason or "").strip()
        if reason:
            detail_label = Label(
                text=f"[color=888888]{reason}[/color]",
                markup=True,
                halign="center",
                valign="middle",
                size_hint_y=None,
            )
            detail_label.bind(
                texture_size=lambda inst, _value: setattr(inst, "height", inst.texture_size[1] + 10),
                width=lambda inst, value: setattr(inst, "text_size", (value, None)),
            )
            content.add_widget(detail_label)

        buttons = BoxLayout(orientation="horizontal", spacing=12, size_hint_y=None, height=52)
        retry_button = Button(text="Erneut versuchen", size_hint=(0.5, None), height=52)
        cancel_button = Button(text="Schließen", size_hint=(0.5, None), height=52)
        buttons.add_widget(retry_button)
        buttons.add_widget(cancel_button)
        content.add_widget(buttons)

        popup = Popup(
            title="Companion-Verbindung",
            content=content,
            size_hint=(0.6, 0.4),
            auto_dismiss=False,
        )
        self._bridge_retry_popup = popup

        retry_button.bind(on_press=lambda *_: self._retry_bridge_connection())
        cancel_button.bind(on_press=lambda *_: popup.dismiss())

        popup.open()

    def _retry_bridge_connection(self) -> None:
        popup = self._bridge_retry_popup
        if popup is not None:
            try:
                popup.dismiss()
            except Exception:
                pass
            self._bridge_retry_popup = None

        bridge = self._bridge
        if bridge is None:
            return

        def _attempt() -> None:
            try:
                bridge.connect()
            except Exception as exc:  # pragma: no cover - network dependent
                error_text = str(exc)
                log.error("Erneuter Companion-Verbindungsversuch fehlgeschlagen: %s", exc)
                self._bridge_connect_error_reason = error_text

                def _show(_dt: float) -> None:
                    self._show_bridge_error_dialog(error_text)

                Clock.schedule_once(_show, 0.0)
            else:
                self._bridge_connect_error_reason = None

                def _apply(_dt: float) -> None:
                    root = cast(Optional[TabletopRoot], self.root)
                    try:
                        connected = set(bridge.connected_players())
                    except Exception:
                        connected = set()
                    if connected:
                        self._players.update(connected)
                    if root is not None:
                        try:
                            root.update_bridge_context(
                                bridge=bridge,
                                players=set(self._players),
                                session=self._session,
                                block=self._block,
                            )
                        except AttributeError:
                            pass

                Clock.schedule_once(_apply, 0.0)

        threading.Thread(target=_attempt, name="BridgeReconnect", daemon=True).start()

    def on_start(self) -> None:  # pragma: no cover - framework callback
        super().on_start()
        root = cast(Optional[TabletopRoot], self.root)

        if root is not None:
            try:
                root.update_bridge_context(
                    bridge=self._bridge,
                    players=set(self._players),
                    session=self._session,
                    block=self._block,
                )
            except AttributeError:
                pass

        self._target_display_index = self._determine_display_index()
        self._apply_display_environment(self._target_display_index)
        if root is not None:
            try:
                root.overlay_display_index = self._target_display_index
            except AttributeError:
                pass

        def _start_overlay_late(_dt: float) -> None:
            process_handle: Optional[OverlayProcess]
            if root and getattr(root, "overlay_process", None):
                process_handle = cast(Optional[OverlayProcess], root.overlay_process)
            else:
                process_handle = self._overlay_process

            try:
                process_handle = start_overlay(
                    process_handle,
                    overlay_path=ARUCO_OVERLAY_PATH,
                    display_index=self._target_display_index,
                )
            except Exception as exc:  # pragma: no cover - safety net
                log.exception("Overlay start failed: %s", exc)
                return

            self._overlay_process = process_handle
            if root is not None:
                root.overlay_process = process_handle
            log.info("Overlay started after fullscreen.")

        def _enter_fullscreen(_dt: float) -> None:
            try:
                self._target_display_index = self._move_window_to_display(
                    self._target_display_index
                )
                if root is not None:
                    try:
                        root.overlay_display_index = self._target_display_index
                    except AttributeError:
                        pass
                Window.borderless = True
                Window.fullscreen = "auto"
                log.info("Fullscreen engaged (auto).")
            except Exception as exc:  # pragma: no cover - safety net
                log.exception("Failed to enter fullscreen: %s", exc)

            self._bind_esc()
            Clock.schedule_once(_start_overlay_late, 0.25)

        Clock.schedule_once(_enter_fullscreen, 0.0)

        if self._perf_logging:
            self._frame_samples.clear()
            self._frame_sampler = Clock.schedule_interval(
                self._track_frame_time, 0
            )
            self._frame_log_event = Clock.schedule_interval(
                self._log_frame_metrics, 10.0
            )
            self._queue_monitor_event = Clock.schedule_interval(
                self._monitor_queues, 1.0
            )

        if self._bridge_connect_error_reason:
            error_text = self._bridge_connect_error_reason

            def _show_dialog(_dt: float) -> None:
                self._show_bridge_error_dialog(error_text or "")

            Clock.schedule_once(_show_dialog, 0.2)

    def on_stop(self) -> None:  # pragma: no cover - framework callback
        root = cast(Optional[TabletopRoot], self.root)

        for event in (self._frame_sampler, self._frame_log_event, self._queue_monitor_event):
            self._cancel_event(event)
        self._frame_sampler = None
        self._frame_log_event = None
        self._queue_monitor_event = None

        process_handle: Optional[OverlayProcess]
        if root and getattr(root, "overlay_process", None):
            process_handle = cast(Optional[OverlayProcess], root.overlay_process)
        else:
            process_handle = self._overlay_process

        process_handle = stop_overlay(process_handle)
        self._overlay_process = process_handle
        if root is not None:
            root.overlay_process = process_handle

        if root is not None:
            logger = getattr(root, "logger", None)
            if logger is not None:
                close_fn = getattr(logger, "close", None)
                if callable(close_fn):
                    close_fn()
                root.logger = None
            flush_round_log(
                root,
                force=True,
                wait=not self._low_latency_disabled,
            )
            close_round_log(root)

        if root is not None:
            shutdown_sync = getattr(root, "shutdown_sync_services", None)
            if callable(shutdown_sync):
                try:
                    shutdown_sync()
                except Exception:  # pragma: no cover - defensive fallback
                    log.debug("shutdown_sync_services raised", exc_info=True)
            try:
                root.stop_bridge_recordings()
            except AttributeError:
                pass
        elif self._bridge is not None:
            for player in self._iter_active_players():
                try:
                    self._bridge.stop_recording(player)
                except Exception:  # pragma: no cover - defensive fallback
                    log.exception("Failed to stop recording for %s", player)

        super().on_stop()

    def abort_block(self) -> None:
        """Abort the active block and discard ongoing recordings."""

        root = cast(Optional[TabletopRoot], self.root)
        if root is not None:
            abort_fn = getattr(root, "abort_block", None)
            if callable(abort_fn):
                abort_fn()
                return

        if self._bridge is not None:
            for player in self._iter_active_players():
                try:
                    self._bridge.recording_cancel(player)
                except Exception:  # pragma: no cover - defensive fallback
                    log.exception("Failed to cancel recording for %s", player)


def _configure_async_logging() -> tuple[Optional[QueueListener], Optional[Queue]]:
    """Install a queue-based logging pipeline if supported."""

    if is_low_latency_disabled():
        return None, None

    log_queue: Queue = Queue(maxsize=4000)
    root_logger = logging.getLogger()
    handlers = list(root_logger.handlers)
    if not handlers:
        console = logging.StreamHandler()
        console.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )
        handlers = [console]

    # Suppress noisy per-response debug logs emitted by the Pupil Labs client while
    # keeping warnings and errors visible.
    for noisy_name in (
        "Response",
        "pupil_labs.realtime_api",
        "pupil_labs.realtime_api.simple",
    ):
        logging.getLogger(noisy_name).setLevel(logging.WARNING)

    for handler in handlers:
        root_logger.removeHandler(handler)

    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)

    listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.daemon = True
    listener.start()
    return listener, log_queue


def _resolve_requested_players(
    player: str, *, connected: Optional[set[str]] = None
) -> list[str]:
    requested = (player or "auto").strip().lower()
    connected = {p for p in (connected or set()) if p}

    if requested == "auto":
        if connected:
            return sorted(connected)
        return ["VP1"]

    if requested == "both":
        return ["VP1", "VP2"]

    normalized = player.upper() if player else "VP1"
    return [normalized]


def run_demo(*, duration: float = 8.0, heartbeat_interval: float = 2.0) -> None:
    """Run a console demo that showcases the whitelisted payload flow."""

    del heartbeat_interval  # kept for backward compatibility; no longer used

    class DemoBridge:
        def __init__(self) -> None:
            self._players = ["VP1", "VP2"]

        def connected_players(self) -> list[str]:
            return list(self._players)

        def event_queue_load(self) -> tuple[int, int]:
            return (0, len(self._players) * 10)

        def send_event(
            self, name: str, player: str, payload: Optional[dict[str, Any]] = None
        ) -> None:
            allowed = {
                "session",
                "block",
                "player",
                "button",
                "phase",
                "round_index",
                "game_player",
                "player_role",
                "accepted",
                "decision",
                "actor",
            }
            filtered = {k: v for k, v in (payload or {}).items() if k in allowed}
            print(f"[demo] send_event {player}: {name} payload={filtered}")

    log.info("Starting minimal sync demo – this runs without the Kivy UI")
    bridge = DemoBridge()
    session_id = "demo-session"
    block_count = max(1, int(duration // 2) or 1)
    phase = "DEMO"
    start_time = time.perf_counter()
    for block in range(1, block_count + 1):
        for player in bridge.connected_players():
            bridge.send_event(
                "sync.block.pre",
                player,
                {
                    "session": session_id,
                    "block": block,
                    "player": player,
                },
            )
        round_count = 2
        for round_index in range(round_count):
            for player in bridge.connected_players():
                payload = {
                    "session": session_id,
                    "block": block,
                    "player": player,
                    "button": "demo",
                    "phase": phase,
                    "round_index": round_index,
                    "game_player": 1 if player == "VP1" else 2,
                    "player_role": 1 if player == "VP1" else 2,
                    "actor": "SYS",
                }
                bridge.send_event("demo.button", player, payload)
                time.sleep(0.1)
        if time.perf_counter() - start_time >= duration:
            break
    log.info("Demo completed")


def main(
    *,
    session: Optional[int] = None,
    block: Optional[int] = None,
    player: str = "auto",
) -> None:
    """Run the tabletop Kivy application with optional Pupil bridge integration."""

    logging_listener, logging_queue = _configure_async_logging()

    bridge = PupilBridge()
    connect_error: Optional[str] = None
    try:
        bridge.connect()
    except Exception as exc:  # pragma: no cover - defensive fallback
        connect_error = str(exc)
        log.error("Companion-Verbindung fehlgeschlagen: %s", exc)
        log.debug("Stacktrace für Companion-Verbindungsfehler", exc_info=True)

    try:
        connected_players = bridge.connected_players()
    except AttributeError:
        connected_players = set()

    desired_players = _resolve_requested_players(player, connected=connected_players)

    single_block_mode = session is not None and block is not None

    app = TabletopApp(
        session=session,
        block=block,
        player=player,
        players=desired_players,
        bridge=bridge,
        single_block_mode=single_block_mode,
        logging_queue=logging_queue,
        bridge_error=connect_error,
    )
    try:
        app.run()
    finally:
        for tracked in desired_players:
            try:
                bridge.stop_recording(tracked)
            except Exception:  # pragma: no cover - defensive fallback
                log.exception("Failed to stop recording during shutdown for %s", tracked)
        try:
            bridge.close()
        except Exception:  # pragma: no cover - defensive fallback
            log.exception("Failed to close Pupil bridge")
        if logging_listener is not None:
            logging_listener.stop()


if __name__ == "__main__":  # pragma: no cover - manual execution
    parser = argparse.ArgumentParser(description="Tabletop experiment UI")
    parser.add_argument("--session", type=int, default=None, help="Session identifier")
    parser.add_argument("--block", type=int, default=None, help="Block identifier")
    parser.add_argument("--player", default="auto", help="Player to control (VP1/VP2/both)")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run the latency/refinement demo instead of launching the UI",
    )
    args = parser.parse_args()
    if args.demo:
        run_demo()
    else:
        main(session=args.session, block=args.block, player=args.player)
