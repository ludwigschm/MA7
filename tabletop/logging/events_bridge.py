from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests

from tabletop.logging.async_bridge import enqueue
from tabletop.logging.pupylabs_cloud import PupylabsCloudLogger

__all__ = ["init_client", "push_async"]

_log = logging.getLogger(__name__)

_session = requests.Session()
_client: Optional[PupylabsCloudLogger] = None


def init_client(
    base_url: str,
    api_key: str,
    timeout_s: float = 2.0,
    max_retries: int = 3,
) -> None:
    """Initialize the shared Pupylabs client used by the UI bridge."""

    global _client
    if not base_url or not api_key:
        _log.debug("Pupylabs client disabled (missing configuration)")
        _client = None
        return
    _client = PupylabsCloudLogger(
        _session,
        base_url,
        api_key,
        timeout_s,
        max_retries,
    )
    _log.info("Pupylabs client initialized for %s", base_url)


def push_async(event: Dict[str, Any]) -> None:
    """Enqueue *event* for asynchronous delivery to Pupylabs Cloud."""

    if _client is None:
        _log.debug("Pupylabs client not initialized; dropping event")
        return

    payload = dict(event or {})

    def _dispatch() -> None:
        try:
            _client.send(payload)
        except Exception as exc:  # pragma: no cover - defensive
            _log.exception("Failed to push event: %r", exc)

    enqueue(_dispatch)

