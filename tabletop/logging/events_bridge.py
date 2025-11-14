from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from core.http_client import get_sync_session
from tabletop.logging.async_bridge import enqueue
from tabletop.logging.pupil_labs_cloud import PupilLabsCloudLogger

__all__ = ["init_client", "push_async"]

_log = logging.getLogger(__name__)

_session = get_sync_session()
_client: Optional[PupilLabsCloudLogger] = None


ALLOWED_KEYS = {
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


def _filter_for_cloud(event: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in event.items() if k in ALLOWED_KEYS}


def init_client(
    base_url: str,
    api_key: str,
    timeout_s: float = 2.0,
    max_retries: int = 3,
) -> None:
    """Initialize the shared Pupil Labs Cloud client used by the UI bridge."""

    global _client
    if not base_url or not api_key:
        _log.debug("Pupil Labs Cloud client disabled (missing configuration)")
        _client = None
        return
    try:
        _client = PupilLabsCloudLogger(
            _session,
            base_url,
            api_key,
            timeout_s,
            max_retries,
        )
    except Exception as exc:  # pragma: no cover - initialization safety
        _log.warning(
            "Pupil Labs Cloud client disabled after initialization failure: %s",
            exc,
        )
        _client = None
        return
    _log.info("Pupil Labs Cloud client initialized for %s", base_url)


def push_async(event: Dict[str, Any]) -> None:
    """Enqueue *event* for asynchronous delivery to the Pupil Labs Cloud."""

    if _client is None:
        _log.debug("Pupil Labs Cloud client not initialized; dropping event")
        return

    payload = dict(event or {})

    def _dispatch() -> None:
        try:
            filtered = _filter_for_cloud(payload)
            _client.send(filtered)
        except Exception as exc:  # pragma: no cover - defensive
            _log.exception("Failed to push event: %r", exc)

    enqueue(_dispatch)

