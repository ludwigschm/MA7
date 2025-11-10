from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict

import requests

__all__ = ["PupylabsCloudLogger"]


class PupylabsCloudLogger:
    """Small synchronous client for forwarding events to Pupylabs Cloud."""

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

    def send(self, event: Dict[str, Any]) -> None:
        """Send *event* to the Pupylabs ingest endpoint with retries."""

        payload: Dict[str, Any] = dict(event or {})
        payload.setdefault("event_id", str(uuid.uuid4()))

        url = f"{self.base_url}/v1/events/ingest"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        delay = 0.1
        for attempt in range(self.max_retries + 1):
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
                        self._log.debug("Pupylabs ingest success: %s", status)
                    return
                if 500 <= status < 600:
                    raise RuntimeError(f"server responded with {status}")
                self._log.warning(
                    "Pupylabs ingest non-success status: %s %s",
                    status,
                    response.text[:200],
                )
                return
            except Exception as exc:  # pragma: no cover - network safety
                if attempt >= self.max_retries:
                    self._log.error(
                        "Pupylabs ingest failed after %s attempts: %r",
                        attempt + 1,
                        exc,
                    )
                    return
                if self._log.isEnabledFor(logging.DEBUG):
                    self._log.debug(
                        "Pupylabs ingest attempt %s failed: %r", attempt + 1, exc
                    )
                time.sleep(delay)
                delay = min(delay * 2, 1.0)

