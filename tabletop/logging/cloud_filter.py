"""Helpers for determining which events should reach Pupillabs Cloud."""

from __future__ import annotations

from typing import Dict, Tuple

__all__ = ["classify_cloud_logging"]


def classify_cloud_logging(action: str, payload: Dict[str, object]) -> Tuple[bool, bool]:
    """Return ``(send_to_cloud, enqueue_marker)`` for a UI event payload."""

    phase = str(payload.get("phase", ""))
    if phase != "action_applied":
        return False, False

    if "button" in payload:
        return True, True

    if action in {"round_start", "session_start"}:
        return True, False

    return False, False
