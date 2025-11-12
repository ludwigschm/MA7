"""Status bar helpers for displaying time synchronisation health."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ColorState = Literal["ok", "warn", "bad"]


@dataclass
class PillState:
    """Simple container describing the visual state of a pill widget."""

    text: str
    color: ColorState


class StatusBar:
    """Track UI-facing state for time synchronisation diagnostics."""

    def __init__(self) -> None:
        self.time_sync = PillState(text=self._format_text(0.0, 0.0), color="ok")

    def update_time_sync(self, offset_ns: int, health: dict[str, float | bool]) -> None:
        """Update the time-sync pill from the latest health sample."""

        offset_ms = float(offset_ns) / 1_000_000.0
        rms_ms = float(health.get("rms_ms", 0.0) or 0.0)
        jump_ms = abs(float(health.get("offset_jump_ms_last", 0.0) or 0.0))
        is_stable = bool(health.get("is_stable", False))

        color: ColorState
        if is_stable:
            color = "ok"
        elif abs(rms_ms) > 3.0 or abs(jump_ms) > 5.0:
            color = "bad"
        else:
            color = "warn"

        self.time_sync = PillState(text=self._format_text(offset_ms, rms_ms), color=color)

    @staticmethod
    def _format_text(offset_ms: float, rms_ms: float) -> str:
        return f"Offset: {offset_ms:+.1f} ms | RMS: {rms_ms:.1f} ms"
