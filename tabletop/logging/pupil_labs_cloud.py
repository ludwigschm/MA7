from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

__all__ = ["PupilLabsCloudLogger"]


class PupilLabsCloudLogger:
    """Small synchronous client for forwarding events to the Pupil Labs Cloud."""

    def __init__(
        self,
        session: requests.Session,
        base_url: str,
        api_key: str,
        timeout_s: float = 2.0,
        max_retries: int = 3,
    ) -> None:
        self._log = logging.getLogger(__name__)
        self.sess = session
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_s
        self.max_retries = max_retries
        self._backoff_min = 0.5
        self._backoff_max = 30.0

    def send(self, event: Dict[str, Any]) -> None:
        """Send *event* to the Pupil Labs Cloud ingest endpoint with retries."""

        # This loop intentionally retries forever – reliability beats latency and
        # no event is ever discarded once handed to the cloud bridge.

        payload: Dict[str, Any] = dict(event or {})

        url = f"{self.base_url}/v1/events/ingest"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        delay = self._backoff_min
        attempt = 0
        event_id = _extract_event_id(payload)
        while True:
            attempt += 1
            try:
                response = self.sess.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )
                status = response.status_code
                if 200 <= status < 300:
                    if self._log.isEnabledFor(logging.DEBUG):
                        self._log.debug(
                            "Pupil Labs Cloud ingest success: %s event_id=%s",
                            status,
                            event_id,
                        )
                    return
                body_preview = response.text[:200]
                self._log.warning(
                    "Pupil Labs Cloud ingest status=%s attempt=%s event_id=%s body=%s",
                    status,
                    attempt,
                    event_id,
                    body_preview,
                )
            except Exception as exc:  # pragma: no cover - network safety
                self._log.exception(
                    "Pupil Labs Cloud ingest error attempt=%s event_id=%s: %r",
                    attempt,
                    event_id,
                    exc,
                )
            time.sleep(delay)
            delay = min(delay * 2.0, self._backoff_max)


def _extract_event_id(payload: Dict[str, Any]) -> Optional[str]:
    properties = payload.get("properties")
    if isinstance(properties, dict) and properties.get("event_id"):
        return str(properties["event_id"])
    if payload.get("event_id"):
        return str(payload["event_id"])
    return None

