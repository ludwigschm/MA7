"""Adapter for game engine event logging."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Optional

from tabletop.engine import EventLogger, Phase as EnginePhase
from core.clock import now_ns

__all__ = ["Events", "EnginePhase"]


class Events:
    """Thin wrapper around :class:`tabletop.engine.EventLogger`."""

    def __init__(self, session_id: str, db_path: str, csv_path: Optional[str] = None):
        self._session_id = session_id
        self._logger = EventLogger(db_path, csv_path)

    def log_event(
        self, payload: Dict[str, Any], *, blocking: bool = False
    ) -> Dict[str, Any]:
        """Persist a structured event payload.

        Parameters
        ----------
        payload:
            Structured event data. ``session_id`` defaults to the configured
            session when omitted. ``engine_phase`` defaults to the empty string
            for backwards compatibility.
        blocking:
            When :class:`True`, the event is synchronously written to disk. This
            mode is also forced when the ``STRICT_LOGGING`` environment variable
            is set to ``"1"``.
        """

        effective_blocking = blocking or os.getenv("STRICT_LOGGING") == "1"
        session_id = payload.get("session_id", self._session_id)
        round_idx = payload.get("round_idx", 0)
        engine_phase = payload.get("engine_phase")
        if isinstance(engine_phase, EnginePhase):
            pass
        elif isinstance(engine_phase, str):
            try:
                engine_phase = EnginePhase[engine_phase]
            except KeyError:
                engine_phase = EnginePhase.WAITING_START
        else:
            engine_phase = EnginePhase.WAITING_START
        actor = payload.get("actor", "SYS")
        action = payload.get("action", "unknown")
        data_payload = dict(payload.get("payload", {}))

        if "event_id" in payload and "event_id" not in data_payload:
            data_payload["event_id"] = payload["event_id"]
        if "phase" in payload and "phase" not in data_payload:
            data_payload["phase"] = payload["phase"]
        if "player" in payload and payload["player"] is not None:
            data_payload.setdefault("player", payload["player"])

        t_ns = payload.get("t_ns")
        if t_ns is None:
            # Backwards compatibility for legacy payloads.
            t_ns = payload.get("t_mono_ns")
        t_utc_iso = payload.get("t_utc_iso")
        if t_ns is None:
            t_ns = now_ns()
        if t_utc_iso is None:
            t_utc_iso = datetime.utcnow().isoformat()

        return self._logger.log(
            session_id,
            round_idx,
            engine_phase,
            actor,
            action,
            data_payload,
            t_ns=t_ns,
            t_utc_iso=t_utc_iso,
            blocking=effective_blocking,
        )

    def log(
        self,
        round_idx: int,
        phase: EnginePhase,
        actor: str,
        action: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        t_ns: Optional[int] = None,
        t_utc_iso: Optional[str] = None,
        blocking: bool = False,
    ) -> Dict[str, Any]:
        """Forward events to the underlying logger while fixing defaults."""

        event_payload: Dict[str, Any] = {
            "session_id": self._session_id,
            "round_idx": round_idx,
            "engine_phase": phase,
            "actor": actor,
            "action": action,
            "payload": payload or {},
        }
        if t_ns is not None:
            event_payload["t_ns"] = t_ns
        if t_utc_iso is not None:
            event_payload["t_utc_iso"] = t_utc_iso
        return self.log_event(event_payload, blocking=blocking)

    def close(self) -> None:
        """Close the underlying logger."""

        self._logger.close()
