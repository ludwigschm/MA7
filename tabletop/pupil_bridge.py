"""Integration helpers for communicating with Pupil Labs devices."""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional, Union

import metrics
from core.capabilities import CapabilityRegistry, DeviceCapabilities
from core.device_registry import DeviceRegistry
from core.event_router import EventRouter, TimestampPolicy, UIEvent, policy_for
from core.recording import DeviceClient, RecordingController, RecordingHttpError
from core.time_sync import TimeSyncManager

from core.http_client import get_sync_session

try:  # pragma: no cover - optional dependency
    from pupil_labs.realtime_api.simple import (
        Device,
        discover_devices,
        discover_one_device,
    )
except Exception:  # pragma: no cover - optional dependency
    Device = None  # type: ignore[assignment]
    discover_devices = None  # type: ignore[assignment]
    discover_one_device = None  # type: ignore[assignment]

from requests import Response

_HTTP_SESSION = get_sync_session()
_JSON_HEADERS = {"Content-Type": "application/json"}
_REST_START_PAYLOAD = json.dumps({"action": "START"}, separators=(",", ":"))


def is_transient(status_code: int) -> bool:
    return status_code in (502, 503, 504)


def _response_preview(response: Response) -> str:
    try:
        body = response.text
    except Exception:
        return ""
    return (body or "")[:120]


log = logging.getLogger(__name__)

def _reachable(ip: str, port: int, timeout: float = 1.5) -> bool:
    try:
        response = _HTTP_SESSION.get(
            f"http://{ip}:{port}/api/status", timeout=timeout
        )
        return bool(response.ok)
    except Exception:
        return False


from tabletop.utils.runtime import (
    event_batch_size_override,
    event_batch_window_override,
    is_low_latency_disabled,
    is_perf_logging_enabled,
)

CONFIG_TEMPLATE = """# Neon Geräte-Konfiguration

VP1_ID=
VP1_IP=192.168.137.121
VP1_PORT=8080

VP2_ID=
VP2_IP=
VP2_PORT=8080
"""

CONFIG_PATH = Path(__file__).resolve().parent.parent / "neon_devices.txt"

_HEX_ID_PATTERN = re.compile(r"([0-9a-fA-F]{16,})")


def _ensure_config_file(path: Path) -> None:
    if path.exists():
        return
    try:
        path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    except Exception:  # pragma: no cover - defensive fallback
        log.exception("Konfigurationsdatei %s konnte nicht erstellt werden", path)


@dataclass
class NeonDeviceConfig:
    player: str
    device_id: str = ""
    ip: str = ""
    port: Optional[int] = None
    port_invalid: bool = False

    @property
    def is_configured(self) -> bool:
        return bool(self.ip)

    @property
    def address(self) -> Optional[str]:
        if not self.ip:
            return None
        if self.port:
            return f"{self.ip}:{self.port}"
        return self.ip

    def summary(self) -> str:
        if not self.is_configured:
            return f"{self.player}(deaktiviert)"
        ip_display = self.ip or "-"
        if self.port_invalid:
            port_display = "?"
        else:
            port_display = str(self.port) if self.port is not None else "-"
        id_display = self.device_id or "-"
        return f"{self.player}(ip={ip_display}, port={port_display}, id={id_display})"


@dataclass
class _QueuedEvent:
    name: str
    player: str
    payload: Optional[Dict[str, Any]]
    priority: Literal["high", "normal"]
    t_ui_ns: int
    t_enqueue_ns: int
    timestamp_policy: TimestampPolicy


class _BridgeDeviceClient(DeviceClient):
    """Adapter exposing async recording operations for :class:`RecordingController`."""

    def __init__(
        self,
        bridge: "PupilBridge",
        player: str,
        device: Any,
        cfg: NeonDeviceConfig,
    ) -> None:
        self._bridge = bridge
        self._player = player
        self._device = device
        self._cfg = cfg

    async def recording_start(self, *, label: str | None = None) -> None:
        def _start() -> None:
            if self._bridge._active_recording.get(self._player):
                raise RecordingHttpError(400, "Already recording!")
            success, _ = self._bridge._invoke_recording_start(
                self._player, self._device
            )
            if not success:
                raise RecordingHttpError(503, "recording start failed", transient=True)
            self._bridge._active_recording[self._player] = True

        await asyncio.to_thread(_start)

    async def recording_begin(self) -> object:
        def _begin() -> object:
            info = self._bridge._wait_for_notification(
                self._device, "recording.begin", timeout=0.5
            )
            if info is None:
                raise asyncio.TimeoutError()
            return info

        return await asyncio.to_thread(_begin)

    async def recording_stop(self) -> None:
        def _stop() -> None:
            stopped = False
            stop_fn = getattr(self._device, "recording_stop", None)
            if callable(stop_fn):
                try:
                    stop_fn()
                    stopped = True
                except Exception:
                    stopped = False
            if not stopped:
                self._bridge._post_device_api(
                    self._player,
                    "/api/recording",
                    {"action": "STOP"},
                    warn=False,
                )
            self._bridge._active_recording[self._player] = False

        await asyncio.to_thread(_stop)

    async def is_recording(self) -> bool:
        return bool(self._bridge._active_recording.get(self._player))

    async def recording_cancel(self) -> None:
        def _cancel() -> None:
            self._bridge.recording_cancel(self._player)

        await asyncio.to_thread(_cancel)

def _load_device_config(path: Path) -> Dict[str, NeonDeviceConfig]:
    configs: Dict[str, NeonDeviceConfig] = {
        "VP1": NeonDeviceConfig("VP1"),
        "VP2": NeonDeviceConfig("VP2"),
    }
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return configs
    except Exception:  # pragma: no cover - defensive fallback
        log.exception("Konfiguration %s konnte nicht gelesen werden", path)
        return configs

    parsed: Dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        parsed[key.strip().upper()] = value.strip()

    vp1 = configs["VP1"]
    vp1.device_id = parsed.get("VP1_ID", vp1.device_id)
    vp1.ip = parsed.get("VP1_IP", vp1.ip).strip()
    vp1_port_raw = parsed.get("VP1_PORT", "").strip()
    if vp1_port_raw:
        try:
            vp1.port = int(vp1_port_raw)
        except ValueError:
            vp1.port_invalid = True
            vp1.port = None
    else:
        vp1.port = 8080

    vp2 = configs["VP2"]
    vp2.device_id = parsed.get("VP2_ID", vp2.device_id)
    vp2.ip = parsed.get("VP2_IP", vp2.ip).strip()
    vp2_port_raw = parsed.get("VP2_PORT", "").strip()
    if vp2_port_raw:
        try:
            vp2.port = int(vp2_port_raw)
        except ValueError:
            vp2.port_invalid = True
            vp2.port = None
    elif vp2.ip:
        vp2.port = 8080

    log.info("[Konfig geladen] %s, %s", vp1.summary(), vp2.summary())

    return configs


_ensure_config_file(CONFIG_PATH)


class PupilBridge:
    """Facade around the Pupil Labs realtime API with graceful fallbacks."""

    DEFAULT_MAPPING: Dict[str, str] = {}
    _PLAYER_INDICES: Dict[str, int] = {"VP1": 1, "VP2": 2}

    def __init__(
        self,
        device_mapping: Optional[Dict[str, str]] = None,
        connect_timeout: float = 10.0,
        *,
        config_path: Optional[Path] = None,
    ) -> None:
        config_file = config_path or CONFIG_PATH
        _ensure_config_file(config_file)
        self._device_config = _load_device_config(config_file)
        mapping_src = device_mapping if device_mapping is not None else self.DEFAULT_MAPPING
        self._device_id_to_player: Dict[str, str] = {
            str(device_id).lower(): player for device_id, player in mapping_src.items() if player
        }
        self._connect_timeout = float(connect_timeout)
        self._http_timeout = max(0.1, min(0.3, float(connect_timeout)))
        self._device_by_player: Dict[str, Any] = {"VP1": None, "VP2": None}
        self._active_recording: Dict[str, bool] = {"VP1": False, "VP2": False}
        self._recording_metadata: Dict[str, Dict[str, Any]] = {}
        self._auto_session: Optional[int] = None
        self._auto_block: Optional[int] = None
        self._auto_players: set[str] = set()
        self._low_latency_disabled = is_low_latency_disabled()
        self._perf_logging = is_perf_logging_enabled()
        self._event_queue_maxsize = 1000
        self._event_queue_drop = 0
        self._queue_sentinel: object = object()
        self._sender_stop = threading.Event()
        self._event_queue: Optional[queue.Queue[object]] = None
        self._sender_thread: Optional[threading.Thread] = None
        self._event_batch_size = event_batch_size_override(4)
        self._event_batch_window = event_batch_window_override(0.005)
        self._last_queue_log = 0.0
        self._last_send_log = 0.0
        self._offset_anomaly_warned: set[str] = set()
        self.ready = threading.Event()
        self._device_registry = DeviceRegistry()
        self._capabilities = CapabilityRegistry()
        self._time_sync: Dict[str, TimeSyncManager] = {}
        self._time_sync_tasks: Dict[str, asyncio.Future[None]] = {}
        self.time_sync: Optional[TimeSyncManager] = None
        self._recording_controllers: Dict[str, RecordingController] = {}
        self._active_router_player: Optional[str] = None
        self._player_device_id: Dict[str, str] = {}
        self._async_loop = asyncio.new_event_loop()
        self._async_thread = threading.Thread(
            target=self._async_loop.run_forever,
            name="PupilBridgeAsync",
            daemon=True,
        )
        self._async_thread.start()
        self._event_router = EventRouter(
            self._on_routed_event,
            batch_interval_s=self._event_batch_window,
            max_batch=self._event_batch_size,
            multi_route=False,
        )
        self._event_router.set_active_player("VP1")
        self._active_router_player = "VP1"
        if not self._low_latency_disabled:
            self._event_queue = queue.Queue(maxsize=self._event_queue_maxsize)
            self._sender_thread = threading.Thread(
                target=self._event_sender_loop,
                name="PupilBridgeSender",
                daemon=True,
            )
            self._sender_thread.start()

    # ---------------------------------------------------------------------
    # Lifecycle management
    def connect(self) -> bool:
        """Discover or configure devices and map them to configured players."""

        self.ready.clear()
        configured_players = {
            player for player, cfg in self._device_config.items() if cfg.is_configured
        }
        if configured_players:
            return self._connect_from_config(configured_players)
        return self._connect_via_discovery()

    def _validate_config(self) -> None:
        vp1 = self._device_config.get("VP1")
        if vp1 is None or not vp1.ip:
            log.error("VP1_IP ist nicht gesetzt – Verbindung wird abgebrochen.")
            raise RuntimeError("VP1_IP fehlt in neon_devices.txt")
        if vp1.port_invalid or vp1.port is None:
            log.error("VP1_PORT ist ungültig – Verbindung wird abgebrochen.")
            raise RuntimeError("VP1_PORT ungültig in neon_devices.txt")
        if vp1.port is None:
            vp1.port = 8080

        vp2 = self._device_config.get("VP2")
        if vp2 and vp2.is_configured and (vp2.port_invalid or vp2.port is None):
            log.error("VP2_PORT ist ungültig – Gerät wird übersprungen.")

    def _connect_from_config(self, configured_players: Iterable[str]) -> bool:
        if Device is None:
            raise RuntimeError(
                "Pupil Labs realtime API not available – direkte Verbindung nicht möglich."
            )

        self._validate_config()

        success = True
        for player in ("VP1", "VP2"):
            cfg = self._device_config.get(player)
            if cfg is None:
                continue
            if not cfg.is_configured:
                if player == "VP2":
                    log.info("VP2(deaktiviert) – keine Verbindung aufgebaut.")
                continue
            if cfg.port_invalid or cfg.port is None:
                message = f"Ungültiger Port für {player}: {cfg.port!r}"
                if player == "VP1":
                    raise RuntimeError(message)
                log.error(message)
                success = False
                continue
            try:
                device = self._connect_device_with_retries(player, cfg)
                actual_id = self._validate_device_identity(device, cfg)
            except Exception as exc:  # pragma: no cover - hardware dependent
                if player == "VP1":
                    raise RuntimeError(f"VP1 konnte nicht verbunden werden: {exc}") from exc
                log.error("Verbindung zu VP2 fehlgeschlagen: %s", exc)
                success = False
                continue

            self._device_by_player[player] = device
            log.info(
                "Verbunden mit %s (ip=%s, port=%s, device_id=%s)",
                player,
                cfg.ip,
                cfg.port,
                actual_id,
            )
            self._on_device_connected(player, device, cfg, actual_id)
            self._auto_start_recording(player, device)

        if "VP1" in configured_players and self._device_by_player.get("VP1") is None:
            raise RuntimeError("VP1 ist konfiguriert, konnte aber nicht verbunden werden.")
        return success and (self._device_by_player.get("VP1") is not None)

    def _connect_device_with_retries(self, player: str, cfg: NeonDeviceConfig) -> Any:
        delays = [1.0, 1.5, 2.0]
        last_error: Optional[BaseException] = None
        for attempt in range(1, 4):
            log.info("Verbinde mit ip=%s, port=%s (Versuch %s/3)", cfg.ip, cfg.port, attempt)
            try:
                device = self._connect_device_once(cfg)
                return self._ensure_device_connection(device)
            except Exception as exc:
                last_error = exc
                log.error("Verbindungsversuch %s/3 für %s fehlgeschlagen: %s", attempt, player, exc)
                if attempt < 3:
                    time.sleep(delays[attempt - 1])
        raise last_error if last_error else RuntimeError("Unbekannter Verbindungsfehler")

    def _connect_device_once(self, cfg: NeonDeviceConfig) -> Any:
        assert Device is not None  # guarded by caller
        if not cfg.ip or cfg.port is None:
            raise RuntimeError("IP oder Port fehlen für den Verbindungsaufbau")

        ip = str(cfg.ip).strip()
        try:
            port = int(cfg.port)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Ungültiger Port-Wert: {cfg.port!r}") from exc

        if not _reachable(ip, port):
            raise RuntimeError(
                f"Kein Companion erreichbar unter http://{ip}:{port}/api/status. "
                "Gleiches Netzwerk? Companion-App aktiv? Firewall?"
            )

        cfg.ip = ip
        cfg.port = port

        first_error: Optional[BaseException] = None
        try:
            return Device(address=ip, port=port)
        except Exception as exc:
            first_error = exc
            log.error(
                "Direkte Verbindung zu %s:%s fehlgeschlagen: %s", ip, port, exc
            )
            if discover_one_device is None and discover_devices is None:
                raise

        fallback_error: Optional[BaseException] = None
        device: Any = None
        if discover_one_device is not None:
            try:
                device = discover_one_device(timeout_seconds=2.0)
            except Exception as exc:
                fallback_error = exc
            else:
                if device is not None:
                    return device

        if discover_devices is None:
            if fallback_error is not None:
                raise fallback_error
            if first_error is not None:
                raise RuntimeError(
                    f"Device konnte nicht initialisiert werden: {first_error}"
                ) from first_error
            raise RuntimeError("Keine Discovery-Funktionen verfügbar")

        try:
            devices = discover_devices(timeout_seconds=2.0)
        except Exception as exc:
            if fallback_error is not None:
                raise RuntimeError(
                    f"Device konnte nicht initialisiert werden: {fallback_error}; {exc}"
                ) from exc
            if first_error is not None:
                raise RuntimeError(
                    f"Device konnte nicht initialisiert werden: {first_error}; {exc}"
                ) from exc
            raise

        if not devices:
            if fallback_error is not None:
                raise fallback_error
            if first_error is not None:
                raise RuntimeError(
                    f"Device konnte nicht initialisiert werden: {first_error}"
                ) from first_error
            raise RuntimeError("Discovery fand kein Companion-Gerät")

        return devices[0]

    def _ensure_device_connection(self, device: Any) -> Any:
        connect_fn = getattr(device, "connect", None)
        if callable(connect_fn):
            try:
                connect_fn()
            except TypeError:
                connect_fn(device)
        return device

    def _close_device(self, device: Any) -> None:
        for attr in ("disconnect", "close"):
            fn = getattr(device, attr, None)
            if callable(fn):
                try:
                    fn()
                except Exception:
                    log.debug("%s() schlug fehl beim Aufräumen", attr, exc_info=True)

    def _validate_device_identity(self, device: Any, cfg: NeonDeviceConfig) -> str:
        status = self._get_device_status(device)
        if status is None and cfg.ip and cfg.port is not None:
            url = f"http://{cfg.ip}:{cfg.port}/api/status"
            try:
                response = _HTTP_SESSION.get(url, timeout=self._connect_timeout)
                response.raise_for_status()
                status = response.json()
            except Exception as exc:
                log.error("HTTP-Statusabfrage %s fehlgeschlagen: %s", url, exc)

        if status is None:
            raise RuntimeError("/api/status konnte nicht abgerufen werden")

        device_id, module_serial = self._extract_identity_fields(status)
        expected_raw = (cfg.device_id or "").strip()
        expected_hex = self._extract_hex_device_id(expected_raw)

        if not expected_raw:
            log.warning(
                "Keine device_id für %s in der Konfiguration gesetzt – Validierung nur über Statusdaten.",
                cfg.player,
            )
        elif not expected_hex:
            log.warning(
                "Konfigurierte device_id %s enthält keine gültige Hex-ID.", expected_raw
            )

        cfg_display = expected_hex or (expected_raw or "-")

        if device_id:
            log.info("device_id=%s bestätigt (cfg=%s)", device_id, cfg_display)
            if expected_hex and device_id.lower() != expected_hex.lower():
                self._close_device(device)
                raise RuntimeError(
                    f"Gefundenes device_id={device_id} passt nicht zu Konfig {cfg_display}"
                )
            if not cfg.device_id:
                cfg.device_id = device_id
            if not self.ready.is_set():
                self.ready.set()
            return device_id

        if module_serial:
            log.info("Kein device_id im Status, nutze module_serial=%s (cfg=%s)", module_serial, cfg_display)
        else:
            log.warning(
                "device_id not present in status; proceeding based on IP/port only (cfg=%s)",
                cfg_display,
            )

        if expected_hex and not device_id:
            log.warning(
                "Konfigurierte device_id %s konnte nicht bestätigt werden.", expected_hex
            )

        if not self.ready.is_set():
            self.ready.set()
        return module_serial or ""

    def _auto_start_recording(self, player: str, device: Any) -> None:
        if self._active_recording.get(player):
            log.info("recording.start übersprungen (%s bereits aktiv)", player)
            return
        label = f"auto.{player.lower()}.{int(time.time())}"
        controller = self._recording_controllers.get(player)
        if controller is None:
            cfg = self._device_config.get(player)
            if cfg is None:
                return
            controller = self._build_recording_controller(player, device, cfg)
            self._recording_controllers[player] = controller

        async def orchestrate() -> Optional[str]:
            await controller.ensure_started(label=label)
            info = await controller.begin_segment()
            return self._extract_recording_id(info) if info is not None else None

        future = asyncio.run_coroutine_threadsafe(orchestrate(), self._async_loop)
        try:
            recording_id = future.result(timeout=max(1.0, self._connect_timeout))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Auto recording start failed for %s: %s", player, exc)
            return

        self._active_recording[player] = True
        self._recording_metadata[player] = {
            "player": player,
            "recording_label": label,
            "event": "auto_start",
            "recording_id": recording_id,
        }

    def _on_device_connected(
        self,
        player: str,
        device: Any,
        cfg: NeonDeviceConfig,
        device_id: str,
    ) -> None:
        endpoint = cfg.address or ""
        if endpoint:
            self._device_registry.confirm(endpoint, device_id)
        self._player_device_id[player] = device_id
        self._event_router.register_player(player)
        if self._active_router_player is None:
            self._event_router.set_active_player(player)
            self._active_router_player = player
        self._setup_time_sync(player, device_id, device)
        self._recording_controllers[player] = self._build_recording_controller(
            player, device, cfg
        )
        self._probe_capabilities(player, cfg, device_id)

    def _setup_time_sync(self, player: str, device_id: str, device: Any) -> None:
        async def measure(samples: int, timeout: float) -> list[float]:
            estimator = getattr(device, "estimate_time_offset", None)
            if not callable(estimator):
                return []
            offsets: list[float] = []
            for _ in range(samples):
                try:
                    value = await asyncio.wait_for(
                        asyncio.to_thread(estimator), timeout
                    )
                except asyncio.TimeoutError:
                    break
                except Exception:
                    break
                else:
                    try:
                        offsets.append(float(value))
                    except Exception:
                        continue
            return offsets

        manager = TimeSyncManager(
            device_id=device_id or player,
            measure_fn=measure,
            max_samples=20,
            sample_timeout=0.25,
        )
        try:
            future = asyncio.run_coroutine_threadsafe(
                manager.initial_sync(), self._async_loop
            )
            future.result(timeout=self._connect_timeout)
        except Exception as exc:
            log.warning("Initial time sync failed for %s: %s", player, exc)
        self._time_sync[player] = manager

    def _schedule_periodic_resync(self, player: str) -> None:
        # Deaktiviert: kein periodischer Re-Sync mehr
        return

    def _build_recording_controller(
        self, player: str, device: Any, cfg: NeonDeviceConfig
    ) -> RecordingController:
        client = _BridgeDeviceClient(self, player, device, cfg)
        logger = logging.getLogger(f"{__name__}.recording.{player.lower()}")
        return RecordingController(client, logger)

    def _probe_capabilities(
        self, player: str, cfg: NeonDeviceConfig, device_id: str
    ) -> None:
        identifier = device_id or player
        supports_frame_name = False
        if cfg.ip and cfg.port is not None:
            url = f"http://{cfg.ip}:{cfg.port}/api/capabilities"
            response: Optional[Response] = None
            try:
                response = _HTTP_SESSION.head(url, timeout=self._http_timeout)
            except Exception as exc:
                log.debug(
                    "Capabilities probe failed for %s endpoint=%s: %s",
                    player,
                    url,
                    exc,
                )
            else:
                if response.status_code == 405:
                    try:
                        response = _HTTP_SESSION.get(
                            url, timeout=self._http_timeout
                        )
                    except Exception as exc:
                        log.debug(
                            "Capabilities probe GET failed for %s endpoint=%s: %s",
                            player,
                            url,
                            exc,
                        )
                        response = None
            if response is not None:
                status = response.status_code
                if status in (200, 204):
                    supports_frame_name = True
                elif 400 <= status < 500:
                    log.info(
                        "Capabilities probe client error endpoint=%s status=%s body=%s",
                        url,
                        status,
                        _response_preview(response),
                    )
        caps = DeviceCapabilities(supports_frame_name=supports_frame_name)
        self._capabilities.set(identifier, caps)
        if not supports_frame_name:
            log.info("frame_name skipped (unsupported) device=%s", player)

    def _get_device_status(self, device: Any) -> Optional[Any]:
        for attr in ("api_status", "status", "get_status"):
            status_fn = getattr(device, attr, None)
            if not callable(status_fn):
                continue
            try:
                result = status_fn()
            except Exception:
                log.debug("Statusabfrage über %s fehlgeschlagen", attr, exc_info=True)
                continue
            if result is None:
                continue
            if isinstance(result, dict):
                return result
            if isinstance(result, (list, tuple)):
                return list(result)
            if isinstance(result, str):
                try:
                    parsed = json.loads(result)
                except json.JSONDecodeError:
                    continue
                else:
                    if isinstance(parsed, (dict, list)):
                        return parsed
            to_dict = getattr(result, "to_dict", None)
            if callable(to_dict):
                try:
                    converted = to_dict()
                except Exception:
                    continue
                if isinstance(converted, (dict, list)):
                    return converted
            as_dict = getattr(result, "_asdict", None)
            if callable(as_dict):
                try:
                    converted = as_dict()
                except Exception:
                    continue
                if isinstance(converted, (dict, list)):
                    return converted
        return None

    def _extract_device_id_from_status(self, status: Any) -> Optional[str]:
        device_id, _ = self._extract_identity_fields(status)
        return device_id

    def _extract_identity_fields(self, status: Any) -> tuple[Optional[str], Optional[str]]:
        device_id: Optional[str] = None
        module_serial: Optional[str] = None

        def set_device(candidate: Any) -> None:
            nonlocal device_id
            if device_id:
                return
            coerced = self._coerce_identity_value(candidate)
            if coerced:
                device_id = coerced

        def set_module(candidate: Any) -> None:
            nonlocal module_serial
            if module_serial:
                return
            coerced = self._coerce_identity_value(candidate)
            if coerced:
                module_serial = coerced

        try:
            if isinstance(status, dict):
                set_device(status.get("device_id"))
                data = status.get("data")
                if isinstance(data, dict):
                    set_device(data.get("device_id"))
                    set_module(data.get("module_serial"))
                set_module(status.get("module_serial"))
            elif isinstance(status, (list, tuple)):
                records = [record for record in status if isinstance(record, dict)]
                for record in records:
                    if record.get("model") == "Phone":
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_device(data.get("device_id"))
                        if device_id:
                            break
                if not device_id:
                    for record in records:
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_device(data.get("device_id"))
                        if device_id:
                            break

                for record in records:
                    if record.get("model") == "Hardware":
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_module(data.get("module_serial"))
                        if module_serial:
                            break
                if not module_serial:
                    for record in records:
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_module(data.get("module_serial"))
                        if module_serial:
                            break
        except Exception:
            log.debug("Konnte Statusinformationen nicht vollständig auswerten", exc_info=True)

        return device_id, module_serial

    def _coerce_identity_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None
        if isinstance(value, str):
            candidate = value.strip()
        else:
            candidate = str(value).strip()
        return candidate or None

    def _extract_hex_device_id(self, value: str) -> Optional[str]:
        if not value:
            return None
        match = _HEX_ID_PATTERN.search(value)
        if not match:
            return None
        return match.group(1).lower()

    def _perform_discovery(self, *, log_errors: bool = True) -> list[Any]:
        if discover_devices is None:
            return []
        try:
            try:
                devices = discover_devices(timeout_seconds=self._connect_timeout)
            except TypeError:
                try:
                    devices = discover_devices(timeout=self._connect_timeout)
                except TypeError:
                    devices = discover_devices(self._connect_timeout)
        except Exception as exc:  # pragma: no cover - network/hardware dependent
            if log_errors:
                log.exception("Failed to discover Pupil devices: %s", exc)
            else:
                log.debug("Discovery fehlgeschlagen: %s", exc, exc_info=True)
            return []
        return list(devices) if devices else []

    def _match_discovered_device(
        self, device_id: str, devices: Optional[Iterable[Any]]
    ) -> Optional[Dict[str, Any]]:
        if not device_id or not devices:
            return None
        wanted = device_id.lower()
        for device in devices:
            info = self._inspect_discovered_device(device)
            candidate = info.get("device_id")
            if candidate and candidate.lower() == wanted:
                return info
        return None

    def _inspect_discovered_device(self, device: Any) -> Dict[str, Any]:
        info: Dict[str, Any] = {"device": device}
        direct_id = self._extract_device_id_attribute(device)
        status: Optional[Dict[str, Any]] = None
        if direct_id:
            info["device_id"] = direct_id
            status = self._get_device_status(device)
        else:
            status = self._get_device_status(device)
            if status is not None:
                status_id = self._extract_device_id_from_status(status)
                if status_id:
                    info["device_id"] = status_id
        if status is None:
            status = {}
        ip, port = self._extract_ip_port(device, status)
        if ip:
            info["ip"] = ip
        if port is not None:
            info["port"] = port
        return info

    def _extract_device_id_attribute(self, device: Any) -> Optional[str]:
        for attr in ("device_id", "id"):
            value = getattr(device, attr, None)
            if value is None:
                continue
            candidate = str(value).strip()
            if candidate:
                return candidate
        return None

    def _extract_ip_port(
        self, device: Any, status: Optional[Any] = None
    ) -> tuple[Optional[str], Optional[int]]:
        for attr in ("address", "ip", "ip_address", "host"):
            value = getattr(device, attr, None)
            ip, port = self._parse_network_value(value)
            if ip:
                return ip, port
        if status:
            dict_sources: list[Dict[str, Any]] = []
            if isinstance(status, dict):
                dict_sources.append(status)
            elif isinstance(status, (list, tuple)):
                for record in status:
                    if isinstance(record, dict):
                        dict_sources.append(record)
                        data = record.get("data")
                        if isinstance(data, dict):
                            dict_sources.append(data)
            for source in dict_sources:
                for path in (
                    ("address",),
                    ("ip",),
                    ("network", "ip"),
                    ("network", "address"),
                    ("system", "ip"),
                    ("system", "address"),
                ):
                    value = self._dig(source, path)
                    ip, port = self._parse_network_value(value)
                    if ip:
                        return ip, port
        return None, None

    def _parse_network_value(self, value: Any) -> tuple[Optional[str], Optional[int]]:
        if value is None:
            return None, None
        if isinstance(value, (list, tuple)):
            if not value:
                return None, None
            host = value[0]
            port = value[1] if len(value) > 1 else None
            return self._coerce_host(host), self._coerce_port(port)
        if isinstance(value, dict):
            host = value.get("host") or value.get("ip") or value.get("address")
            port = value.get("port")
            return self._coerce_host(host), self._coerce_port(port)
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None, None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None, None
            if "//" in text:
                text = text.split("//", 1)[-1]
            if ":" in text:
                host_part, _, port_part = text.rpartition(":")
                host = host_part.strip() or None
                port = self._coerce_port(port_part)
                return host, port
            return text, None
        return self._coerce_host(value), None

    def _coerce_host(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None
        host = str(value).strip()
        return host or None

    def _coerce_port(self, value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _dig(self, data: Dict[str, Any], path: Iterable[str]) -> Any:
        current: Any = data
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    def _connect_via_discovery(self) -> bool:
        if discover_devices is None:
            log.warning(
                "Pupil Labs realtime API not available. Running without device integration."
            )
            return False

        found_devices = self._perform_discovery(log_errors=True)
        if not found_devices:
            log.warning("No Pupil devices discovered within %.1fs", self._connect_timeout)
            return False

        for device in found_devices:
            info = self._inspect_discovered_device(device)
            device_id = info.get("device_id")
            if not device_id:
                log.debug("Skipping device ohne device_id: %r", device)
                continue
            player = self._device_id_to_player.get(device_id.lower())
            if not player:
                log.info("Ignoring unmapped device with device_id %s", device_id)
                continue
            cfg = NeonDeviceConfig(player=player, device_id=device_id)
            cfg.ip = (info.get("ip") or "").strip()
            port_info = info.get("port")
            if isinstance(port_info, str):
                try:
                    cfg.port = int(port_info)
                except ValueError:
                    cfg.port = None
            else:
                cfg.port = port_info
            if cfg.ip and cfg.port is None:
                cfg.port = 8080
            try:
                prepared = self._ensure_device_connection(device)
                actual_id = self._validate_device_identity(prepared, cfg)
                self._device_by_player[player] = prepared
                log.info(
                    "Verbunden mit %s (ip=%s, port=%s, device_id=%s)",
                    player,
                    cfg.ip or "-",
                    cfg.port,
                    actual_id,
                )
                self._on_device_connected(player, prepared, cfg, actual_id)
                self._auto_start_recording(player, prepared)
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning("Gerät %s konnte nicht verbunden werden: %s", device_id, exc)

        missing_players = [player for player, device in self._device_by_player.items() if device is None]
        if missing_players:
            log.warning(
                "No device found for players: %s", ", ".join(sorted(missing_players))
            )
        return self._device_by_player.get("VP1") is not None

    def close(self) -> None:
        """Close all connected devices if necessary."""

        self._event_router.flush_all()
        for future in list(self._time_sync_tasks.values()):
            future.cancel()
            try:
                future.result()
            except Exception:
                pass
        self._time_sync_tasks.clear()
        self._time_sync.clear()
        if self._async_loop.is_running():
            async def _cancel_all() -> None:
                for task in asyncio.all_tasks():
                    if task is not asyncio.current_task():
                        task.cancel()

            stopper = asyncio.run_coroutine_threadsafe(_cancel_all(), self._async_loop)
            try:
                stopper.result()
            except Exception:
                pass
        if self._async_loop.is_running():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._async_thread.is_alive():
            self._async_thread.join(timeout=1.0)
        try:
            self._async_loop.close()
        except RuntimeError:
            pass
        if self._event_queue is not None:
            self._sender_stop.set()
            try:
                self._event_queue.put_nowait(self._queue_sentinel)
            except queue.Full:
                self._event_queue.put(self._queue_sentinel)
            if self._sender_thread is not None:
                self._sender_thread.join(timeout=1.0)
            self._event_queue = None
            self._sender_thread = None
        for player, device in list(self._device_by_player.items()):
            if device is None:
                continue
            try:
                close_fn = getattr(device, "close", None)
                if callable(close_fn):
                    close_fn()
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.exception("Failed to close device for %s: %s", player, exc)
            finally:
                self._device_by_player[player] = None
        for player in list(self._active_recording):
            self._active_recording[player] = False
        self._recording_metadata.clear()

    # ------------------------------------------------------------------
    # Recording helpers
    def ensure_recordings(
        self,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
        players: Optional[Iterable[str]] = None,
    ) -> set[str]:
        if session is not None:
            self._auto_session = session
        if block is not None:
            self._auto_block = block
        if players is not None:
            self._auto_players = {p for p in players if p}

        if self._auto_players:
            target_players = self._auto_players
        else:
            target_players = {p for p, dev in self._device_by_player.items() if dev is not None}

        if self._auto_session is None or self._auto_block is None:
            return set()

        started: set[str] = set()
        for player in target_players:
            self.start_recording(self._auto_session, self._auto_block, player)
            if self._active_recording.get(player):
                started.add(player)
        return started

    def start_recording(self, session: int, block: int, player: str) -> None:
        """Start a recording for the given player using the agreed label schema."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.start übersprungen (%s nicht verbunden)", player)
            return

        recording_label = self._format_recording_label(session, block, player)

        if self._active_recording.get(player):
            self._update_recording_label(player, device, session, block, recording_label)
            return

        controller = self._recording_controllers.get(player)
        if controller is None:
            cfg = self._device_config.get(player)
            if cfg is None:
                log.info("recording.start übersprungen (%s ohne Konfig)", player)
                return
            controller = self._build_recording_controller(player, device, cfg)
            self._recording_controllers[player] = controller

        log.info(
            "recording start requested player=%s label=%s session=%s block=%s",
            player,
            recording_label,
            session,
            block,
        )

        async def orchestrate() -> Optional[str]:
            await controller.ensure_started(label=recording_label)
            info = await controller.begin_segment()
            return self._extract_recording_id(info) if info is not None else None

        future = asyncio.run_coroutine_threadsafe(orchestrate(), self._async_loop)
        try:
            recording_id = future.result(timeout=max(1.0, self._connect_timeout))
        except RecordingHttpError as exc:
            log.warning(
                "recording start failed player=%s status=%s msg=%s",
                player,
                exc.status,
                exc.message,
            )
            return
        except asyncio.TimeoutError:
            log.warning("recording start timeout player=%s", player)
            return
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("recording start error player=%s error=%s", player, exc)
            return

        payload = {
            "session": session,
            "block": block,
            "player": player,
            "recording_label": recording_label,
            "recording_id": recording_id,
        }
        self._active_recording[player] = True
        self._recording_metadata[player] = payload
        self._update_recording_label(player, device, session, block, recording_label)
        self.send_event("session.recording_started", player, payload)

    def is_recording(self, player: str) -> bool:
        """Return whether the player currently has an active recording."""

        return bool(self._active_recording.get(player))

    def _format_recording_label(self, session: int, block: int, player: str) -> str:
        vp_index = self._PLAYER_INDICES.get(player, 0)
        return f"{session}.{block}.{vp_index}"

    def _update_recording_label(
        self,
        player: str,
        device: Any,
        session: int,
        block: int,
        label: str,
    ) -> None:
        """Refresh the recording label for an already active recording."""

        log.info(
            "recording label update requested player=%s label=%s session=%s block=%s",
            player,
            label,
            session,
            block,
        )
        self._apply_recording_label(
            player,
            device,
            label,
            session=session,
            block=block,
        )

        metadata = self._recording_metadata.get(player)
        if metadata is None:
            metadata = {"player": player}
            self._recording_metadata[player] = metadata
        metadata["session"] = session
        metadata["block"] = block
        metadata["recording_label"] = label

    def _send_recording_start(
        self,
        player: str,
        device: Any,
        label: str,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Optional[Any]:
        success, _ = self._invoke_recording_start(player, device)
        if not success:
            return None

        self._apply_recording_label(player, device, label, session=session, block=block)

        begin_info = self._wait_for_notification(device, "recording.begin")
        if begin_info is None:
            return None
        return begin_info

    def _invoke_recording_start(
        self,
        player: str,
        device: Any,
        *,
        allow_busy_recovery: bool = True,
    ) -> tuple[bool, Optional[Any]]:
        start_methods = ("recording_start", "start_recording")
        for method_name in start_methods:
            start_fn = getattr(device, method_name, None)
            if not callable(start_fn):
                continue
            try:
                return True, start_fn()
            except TypeError:
                log.debug(
                    "recording start via %s requires unsupported arguments (%s)",
                    method_name,
                    player,
                    exc_info=True,
                )
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.exception(
                    "Failed to start recording for %s via %s: %s",
                    player,
                    method_name,
                    exc,
                )
                return False, None

        rest_status, rest_payload = self._start_recording_via_rest(player)
        if rest_status == "busy" and allow_busy_recovery:
            if self._handle_busy_state(player, device):
                return self._invoke_recording_start(player, device, allow_busy_recovery=False)
            return False, None
        if rest_status is True:
            return True, rest_payload
        log.error("No recording start method succeeded for %s", player)
        return False, None

    def _start_recording_via_rest(
        self, player: str
    ) -> tuple[Optional[Union[str, bool]], Optional[Any]]:
        cfg = self._device_config.get(player)
        if cfg is None or not cfg.ip or cfg.port is None:
            log.debug("REST recording start skipped (%s: no IP/port)", player)
            return False, None

        url = f"http://{cfg.ip}:{cfg.port}/api/recording"
        response: Optional[Response] = None
        last_exc: Optional[Exception] = None
        delay = 0.05
        attempts = 3
        for attempt in range(attempts):
            try:
                response = _HTTP_SESSION.post(
                    url,
                    data=_REST_START_PAYLOAD,
                    headers=_JSON_HEADERS,
                    timeout=self._http_timeout,
                )
            except Exception as exc:  # pragma: no cover - network dependent
                last_exc = exc
                response = None
            else:
                status = response.status_code
                if status == 200:
                    try:
                        return True, response.json()
                    except ValueError:
                        return True, None
                if is_transient(status) and attempt < attempts - 1:
                    time.sleep(delay)
                    delay *= 2
                    continue
                break
            if attempt < attempts - 1:
                time.sleep(delay)
                delay *= 2

        if response is None:
            if last_exc is not None:
                log.error("REST recording start failed for %s: %s", player, last_exc)
            return False, None

        message: Optional[str] = None
        try:
            data = response.json()
            if isinstance(data, dict):
                message = str(data.get("message") or data.get("error") or "")
        except ValueError:
            message = response.text

        status = response.status_code
        if (
            status == 400
            and message
            and "previous recording not completed" in message.lower()
        ):
            log.warning(
                "Recording start busy player=%s endpoint=%s status=%s body=%s",
                player,
                response.url,
                status,
                _response_preview(response),
            )
            return "busy", None

        log.error(
            "REST recording start failed player=%s endpoint=%s status=%s body=%s",
            player,
            response.url,
            status,
            _response_preview(response),
        )
        return False, None

    def _handle_busy_state(self, player: str, device: Any) -> bool:
        log.info("Attempting to clear busy recording state for %s", player)
        stopped = False
        stop_fn = getattr(device, "recording_stop_and_save", None)
        if callable(stop_fn):
            try:
                stop_fn()
                stopped = True
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning(
                    "recording_stop_and_save failed for %s: %s",
                    player,
                    exc,
                )

        if not stopped:
            response = self._post_device_api(
                player,
                "/api/recording",
                {"action": "STOP"},
                warn=False,
            )
            if response is not None and response.status_code == 200:
                stopped = True
            elif response is not None:
                log.warning(
                    "REST recording stop failed player=%s endpoint=%s status=%s body=%s",
                    player,
                    response.url,
                    response.status_code,
                    _response_preview(response),
                )

        if not stopped:
            return False

        end_info = self._wait_for_notification(device, "recording.end")
        if end_info is None:
            log.warning("Timeout while waiting for recording.end (%s)", player)
        return True

    def _post_device_api(
        self,
        player: str,
        path: str,
        payload: Dict[str, Any],
        *,
        timeout: Optional[float] = None,
        warn: bool = True,
    ) -> Optional[Response]:
        cfg = self._device_config.get(player)
        if cfg is None or not cfg.ip or cfg.port is None:
            if warn:
                log.warning(
                    "REST endpoint %s not configured for %s",
                    path,
                    player,
                )
            return None

        url = f"http://{cfg.ip}:{cfg.port}{path}"
        effective_timeout = timeout or self._http_timeout
        delay = 0.05
        attempts = 3
        last_exc: Optional[Exception] = None
        try:
            payload_json = json.dumps(payload, separators=(",", ":"), default=str)
        except TypeError:
            safe_payload = self._stringify_payload(payload)
            payload_json = json.dumps(safe_payload, separators=(",", ":"))

        response: Optional[Response] = None
        for attempt in range(attempts):
            should_retry = False
            try:
                response = _HTTP_SESSION.post(
                    url,
                    data=payload_json,
                    headers=_JSON_HEADERS,
                    timeout=effective_timeout,
                )
            except Exception as exc:  # pragma: no cover - network dependent
                last_exc = exc
                response = None
                should_retry = True
            else:
                status = response.status_code
                labels = {"player": player, "path": path}
                if 500 <= status < 600:
                    metrics.inc("http_5xx_total", **labels)
                elif 400 <= status < 500:
                    metrics.inc("http_4xx_total", **labels)
                if is_transient(status) and attempt < attempts - 1:
                    should_retry = True
                    response = None
                else:
                    return response

            if should_retry and attempt < attempts - 1:
                metrics.inc("http_retries_total", player=player, path=path)
                time.sleep(delay)
                delay *= 2
                continue

            if response is not None:
                return response

        if warn:
            message = last_exc if last_exc is not None else "no response"
            log.warning("HTTP POST %s failed for %s: %s", url, player, message)
        else:
            log.debug(
                "HTTP POST %s failed for %s: %s",
                url,
                player,
                last_exc if last_exc is not None else "no response",
                exc_info=last_exc is not None,
            )
        return None

    def _apply_recording_label(
        self,
        player: str,
        device: Any,
        label: str,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
    ) -> None:
        if not label:
            return

        identifier = self._player_device_id.get(player, "") or player
        caps = self._capabilities.get(identifier)
        if not caps.supports_frame_name:
            log.debug("frame_name skipped (unsupported) device=%s", player)

        payload: Dict[str, Any] = {"label": label}
        if session is not None:
            payload["session"] = session
        if block is not None:
            payload["block"] = block

        event_fn = getattr(device, "send_event", None)
        if callable(event_fn):
            try:
                event_fn(name="recording.label", payload=payload)
                return
            except TypeError:
                try:
                    event_fn("recording.label", payload)
                    return
                except Exception:
                    pass
            except Exception:
                pass

        try:
            self.send_event("recording.label", player, payload)
        except Exception:
            log.debug("recording.label event fallback failed for %s", player, exc_info=True)

    def _wait_for_notification(
        self, device: Any, event: str, timeout: float = 5.0
    ) -> Optional[Any]:
        waiters = ["wait_for_notification", "wait_for_event", "await_notification"]
        for attr in waiters:
            wait_fn = getattr(device, attr, None)
            if callable(wait_fn):
                try:
                    return wait_fn(event, timeout=timeout)
                except TypeError:
                    return wait_fn(event, timeout)
                except TimeoutError:
                    return None
                except Exception:
                    log.debug("Warten auf %s via %s fehlgeschlagen", event, attr, exc_info=True)
        return None

    def _extract_recording_id(self, info: Any) -> Optional[str]:
        if isinstance(info, dict):
            for key in ("recording_id", "id", "uuid"):
                value = info.get(key)
                if value:
                    return str(value)
        return None

    def recording_cancel(self, player: str) -> None:
        """Abort an active recording for *player* if possible."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.cancel übersprungen (%s: nicht konfiguriert/verbunden)", player)
            self._active_recording[player] = False
            self._recording_metadata.pop(player, None)
            controller = self._recording_controllers.get(player)
            if controller is not None:
                try:
                    controller._active = False  # type: ignore[attr-defined]
                except Exception:
                    pass
            return

        log.info("recording.cancel (%s)", player)

        cancelled = False
        cancel_methods = ("recording_cancel", "recording_stop_and_discard", "cancel_recording")
        for method_name in cancel_methods:
            cancel_fn = getattr(device, method_name, None)
            if not callable(cancel_fn):
                continue
            try:
                cancel_fn()
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning(
                    "recording cancel via %s failed for %s: %s",
                    method_name,
                    player,
                    exc,
                )
            else:
                cancelled = True
                break

        if not cancelled:
            response = self._post_device_api(
                player,
                "/api/recording",
                {"action": "CANCEL"},
                warn=False,
            )
            if response is not None:
                status = response.status_code
                if status in (200, 202, 204):
                    cancelled = True
                elif status == 400:
                    preview = _response_preview(response).lower()
                    if "no recording" in preview or "not recording" in preview:
                        cancelled = True
                    else:
                        log.warning(
                            "REST recording cancel failed player=%s endpoint=%s status=%s body=%s",
                            player,
                            response.url,
                            status,
                            _response_preview(response),
                        )
                else:
                    log.warning(
                        "REST recording cancel failed player=%s endpoint=%s status=%s body=%s",
                        player,
                        response.url,
                        status,
                        _response_preview(response),
                    )

        if cancelled:
            cancel_info = self._wait_for_notification(device, "recording.cancelled", timeout=0.5)
            if cancel_info is None:
                cancel_info = self._wait_for_notification(device, "recording.canceled", timeout=0.5)
            if cancel_info is None:
                self._wait_for_notification(device, "recording.end", timeout=0.5)
        else:
            log.warning("recording.cancel nicht bestätigt (%s)", player)

        self._active_recording[player] = False
        self._recording_metadata.pop(player, None)
        controller = self._recording_controllers.get(player)
        if controller is not None:
            try:
                controller._active = False  # type: ignore[attr-defined]
            except Exception:
                pass

    def stop_recording(self, player: str) -> None:
        """Stop the active recording for the player if possible."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.stop übersprungen (%s: nicht konfiguriert/verbunden)", player)
            return

        if not self._active_recording.get(player):
            log.debug("No active recording to stop for %s", player)
            return

        log.info("recording.stop (%s)", player)

        stop_payload = dict(self._recording_metadata.get(player, {"player": player}))
        stop_payload["player"] = player
        stop_payload["event"] = "stop"
        self.send_event(
            "session.recording_stopped",
            player,
            stop_payload,
        )
        try:
            stop_fn = getattr(device, "recording_stop_and_save", None)
            if callable(stop_fn):
                stop_fn()
            else:
                log.warning("Device for %s lacks recording_stop_and_save", player)
        except Exception as exc:  # pragma: no cover - hardware dependent
            log.exception("Failed to stop recording for %s: %s", player, exc)
            return

        end_info = self._wait_for_notification(device, "recording.end")
        if end_info is not None:
            recording_id = self._extract_recording_id(end_info)
            log.info("recording.end empfangen (%s, id=%s)", player, recording_id or "?")
        else:
            log.info("recording.end nicht bestätigt (%s)", player)

        if player in self._active_recording:
            self._active_recording[player] = False
        self._recording_metadata.pop(player, None)

    def connected_players(self) -> list[str]:
        """Return the players that currently have a connected device."""

        return [
            player
            for player, device in self._device_by_player.items()
            if device is not None
        ]

    # ------------------------------------------------------------------
    # Event helpers
    def _event_sender_loop(self) -> None:
        """Background worker that batches UI events before dispatching."""
        if self._event_queue is None:
            return
        while True:
            try:
                item = self._event_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if item is self._queue_sentinel or self._sender_stop.is_set():
                self._event_queue.task_done()
                break
            if not isinstance(item, _QueuedEvent):
                self._event_queue.task_done()
                continue
            batch: list[_QueuedEvent] = [item]
            deadline = time.perf_counter() + self._event_batch_window
            while len(batch) < self._event_batch_size:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                try:
                    next_item = self._event_queue.get(timeout=max(remaining, 0.0))
                except queue.Empty:
                    break
                if next_item is self._queue_sentinel:
                    self._event_queue.task_done()
                    self._sender_stop.set()
                    break
                if self._sender_stop.is_set():
                    self._event_queue.task_done()
                    break
                if isinstance(next_item, _QueuedEvent):
                    batch.append(next_item)
            self._flush_event_batch(batch)
            for _ in batch:
                self._event_queue.task_done()
            if self._sender_stop.is_set():
                break
        # Drain any remaining events to avoid dropping on shutdown
        while self._event_queue is not None and not self._event_queue.empty():
            try:
                item = self._event_queue.get_nowait()
            except queue.Empty:
                break
            if item is not self._queue_sentinel and isinstance(item, _QueuedEvent):
                self._flush_event_batch([item])
            self._event_queue.task_done()

    def _flush_event_batch(self, batch: list[_QueuedEvent]) -> None:
        """Send a batch of queued events sequentially."""
        if not batch:
            return
        start = time.perf_counter()
        for event in batch:
            self._dispatch_with_metrics(event)
        if self._perf_logging:
            duration = (time.perf_counter() - start) * 1000.0
            if time.monotonic() - self._last_send_log >= 1.0:
                log.debug(
                    "Pupil event batch sent %d events in %.2f ms", len(batch), duration
                )
                self._last_send_log = time.monotonic()

    def _dispatch_event(self, event: _QueuedEvent) -> None:
        """Send a single event to the device, falling back between APIs."""
        name = event.name
        player = event.player
        device = self._device_by_player.get(player)
        if device is None:
            return

        event_label = name
        payload_json: Optional[str] = None
        prepared_payload: Dict[str, Any] = dict(event.payload or {})
        include_timestamp = event.timestamp_policy is TimestampPolicy.CLIENT_CORRECTED
        offset_ns = 0
        if include_timestamp:
            offset_ns = self.get_device_offset_ns(player)
            if abs(offset_ns) > 5_000_000_000:
                if player not in self._offset_anomaly_warned:
                    log.warning(
                        "Large clock offset for %s (offset_ns=%d) – keeping client-side timestamps and triggering resync",
                        player,
                        offset_ns,
                    )
                    self._offset_anomaly_warned.add(player)
                # Sofortige Re-Sync-Anfrage anstoßen (non-blocking best effort)
                manager = self._time_sync.get(player)
                try:
                    if manager is not None:
                        # bevorzugt: asap einmalig messen
                        asyncio.run_coroutine_threadsafe(
                            manager.maybe_resync(), self._async_loop
                        )
                except Exception:
                    pass
        if include_timestamp:
            host_now_unix_ns = time.time_ns()
            prepared_payload.pop("timestamp_ns", None)
            prepared_payload["event_timestamp_unix_ns"] = host_now_unix_ns - offset_ns
        else:
            prepared_payload.pop("timestamp_ns", None)
            prepared_payload.pop("event_timestamp_unix_ns", None)

        if prepared_payload:
            try:
                payload_json = json.dumps(
                    prepared_payload, separators=(",", ":"), default=str
                )
            except TypeError:
                safe_payload = self._stringify_payload(prepared_payload)
                payload_json = json.dumps(safe_payload, separators=(",", ":"))
            event_label = f"{name}|{payload_json}"

        try:
            device.send_event(event_label)
            return
        except TypeError:
            pass
        except Exception as exc:  # pragma: no cover - hardware dependent
            log.exception("Failed to send event %s for %s: %s", name, player, exc)
            return

        try:
            device.send_event(name, prepared_payload)
        except Exception as exc:  # pragma: no cover - hardware dependent
            log.exception("Failed to send event %s for %s: %s", name, player, exc)

    def event_queue_load(self) -> tuple[int, int]:
        if self._event_queue is None:
            return (0, 0)
        return (self._event_queue.qsize(), self._event_queue_maxsize)

    def _dispatch_with_metrics(self, event: _QueuedEvent) -> None:
        try:
            self._dispatch_event(event)
        finally:
            t_dispatch_ns = time.perf_counter_ns()
            self._log_dispatch_latency(event, t_dispatch_ns)

    def _on_routed_event(self, player: str, event: UIEvent) -> None:
        payload_dict = dict(event.payload or {})
        prepared_payload = self._normalise_event_payload(payload_dict)
        t_ui_ns = time.perf_counter_ns()
        event_priority: Literal["high", "normal"]
        if event.priority == "high" or event.name.startswith(("sync.", "fix.")):
            event_priority = "high"
        else:
            event_priority = "normal"
        enqueue_ns = time.perf_counter_ns()
        queued = _QueuedEvent(
            name=event.name,
            player=player,
            payload=prepared_payload,
            priority=event_priority,
            t_ui_ns=int(t_ui_ns),
            t_enqueue_ns=enqueue_ns,
            timestamp_policy=event.timestamp_policy,
        )
        if self._low_latency_disabled or self._event_queue is None or event_priority == "high":
            self._dispatch_with_metrics(queued)
            return
        try:
            self._event_queue.put_nowait(queued)
        except queue.Full:
            self._event_queue_drop += 1
            log.warning(
                "Dropping Pupil event %s for %s – queue full (%d drops)",
                event.name,
                player,
                self._event_queue_drop,
            )
            self._dispatch_with_metrics(queued)
        else:
            if self._perf_logging and self._event_queue.maxsize:
                load = self._event_queue.qsize() / self._event_queue.maxsize
                if load >= 0.8 and time.monotonic() - self._last_queue_log >= 1.0:
                    log.warning(
                        "Pupil event queue at %.0f%% capacity",
                        load * 100.0,
                    )
                    self._last_queue_log = time.monotonic()

    def _log_dispatch_latency(
        self, event: _QueuedEvent, t_dispatch_ns: int
    ) -> None:
        if not self._perf_logging:
            return
        log.debug(
            "bridge latency %s/%s priority=%s t_ui=%d t_enqueue=%d t_dispatch=%d",
            event.player,
            event.name,
            event.priority,
            event.t_ui_ns,
            event.t_enqueue_ns,
            t_dispatch_ns,
        )

    def send_event(
        self,
        name: str,
        player: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        priority: Literal["high", "normal"] = "normal",
        use_arrival_time: bool | None = None,
        policy: TimestampPolicy = TimestampPolicy.CLIENT_CORRECTED,
    ) -> None:
        """Send an event to the player's device, encoding payload as JSON suffix."""

        event_payload = self._normalise_event_payload(payload)
        event_priority: Literal["high", "normal"] = (
            "high" if priority == "high" else "normal"
        )
        if use_arrival_time is not None:
            policy = (
                TimestampPolicy.ARRIVAL
                if use_arrival_time
                else TimestampPolicy.CLIENT_CORRECTED
            )
        elif policy is TimestampPolicy.CLIENT_CORRECTED:
            policy = policy_for(name)
        if not self.ready.is_set():
            if event_priority == "high":
                if not self.ready.wait(0.25):
                    log.warning(
                        "PupilBridge not ready; dropping HIGH event: %s", name
                    )
                    return
            else:
                log.warning(
                    "PupilBridge not ready; dropping NORMAL event: %s", name
                )
                return
        assert self.ready.is_set()
        ui_event = UIEvent(
            name=name,
            payload=event_payload,
            target=player,
            priority=event_priority,
            timestamp_policy=policy,
        )
        self._event_router.register_player(player)
        self._event_router.route(ui_event)

    def send_host_mirror(
        self,
        player: str,
        event_id: str,
        t_host_ns: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        return

    def refine_event(
        self,
        player: str,
        event_id: str,
        t_ref_ns: int,
        *,
        confidence: float,
        mapping_version: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        return

    def _normalise_event_payload(
        self, payload: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Filter outgoing payloads to contain only whitelisted event keys."""

        allowed = {
            "session",
            "block",
            "player",
            "recording_id",
            "button",
            "phase",
            "round_index",
            "game_player",
            "player_role",
            "accepted",
            "decision",
            "actor",
        }
        data: Dict[str, Any] = {}
        if isinstance(payload, dict):
            data.update({k: v for k, v in payload.items() if k in allowed})
        return data

    # ------------------------------------------------------------------
    def get_device_offset_ns(self, player: str) -> int:
        manager = self._time_sync.get(player)
        if manager is None:
            injected = getattr(self, "time_sync", None)
            if isinstance(injected, TimeSyncManager):
                manager = injected
        if manager is None:
            return 0
        offset_fn = getattr(manager, "get_offset_ns", None)
        if callable(offset_fn):
            try:
                offset_value = int(offset_fn())
            except Exception:
                offset_value = 0
        else:
            try:
                offset_value = int(round(-manager.get_offset_s() * 1_000_000_000))
            except Exception:
                offset_value = 0
        return offset_value

    def estimate_time_offset(self, player: str) -> Optional[float]:
        """Return device_time - host_time in seconds if available.

        The reconciler consumes the value as an offset in nanoseconds
        (device_time minus host_time, i.e. positive if the device clock
        runs ahead of the host).  The realtime API does not document the
        polarity, so we emit a warning once per player and let the
        reconciler lock the sign if required.
        """

        manager = self._time_sync.get(player)
        if manager is None:
            device = self._device_by_player.get(player)
            if device is None:
                return None
            device_id = self._player_device_id.get(player, "") or player
            self._setup_time_sync(player, device_id, device)
            manager = self._time_sync.get(player)
            if manager is None:
                return None
        try:
            asyncio.run_coroutine_threadsafe(
                manager.maybe_resync(), self._async_loop
            )
        except Exception:
            pass
        return manager.get_offset_s()

    def is_connected(self, player: str) -> bool:
        """Return whether the given player has an associated device."""

        return self._device_by_player.get(player) is not None

    # ------------------------------------------------------------------
    @staticmethod
    def _stringify_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Convert non-serialisable payload entries to strings."""

        result: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                result[key] = value
            elif isinstance(value, dict):
                result[key] = PupilBridge._stringify_payload(value)  # type: ignore[arg-type]
            elif isinstance(value, (list, tuple)):
                result[key] = [PupilBridge._coerce_item(item) for item in value]
            else:
                result[key] = str(value)
        return result

    @staticmethod
    def _coerce_item(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return PupilBridge._stringify_payload(value)  # type: ignore[arg-type]
        if isinstance(value, (list, tuple)):
            return [PupilBridge._coerce_item(item) for item in value]
        return str(value)


__all__ = ["PupilBridge"]


