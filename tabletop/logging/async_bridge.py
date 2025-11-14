"""Minimal async dispatch queue for bridge calls.

MA2 Bridge-Calls asynchronisiert (MA3-Pattern), Payload vollständig erhalten.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Callable

_log = logging.getLogger(__name__)

_q: "queue.Queue[Callable[[], None]]" = queue.Queue(maxsize=10000)


def _worker() -> None:
    while True:
        try:
            fn = _q.get()
            fn()
        except Exception:  # pragma: no cover - defensive fallback
            _log.exception("async task failed")
        finally:
            _q.task_done()


_thread = threading.Thread(target=_worker, name="AsyncBridge", daemon=True)
_thread.start()


def enqueue(fn: Callable[[], None]) -> None:
    """Schedule *fn* for background execution without blocking the UI."""

    if fn is None:
        return
    # Wait for queue capacity; if pressure persists we run synchronously to keep
    # the guarantee that no event gets dropped on the floor.
    try:
        _q.put(fn, timeout=1.0)
    except queue.Full:
        _log.warning(
            "async queue saturated – executing task synchronously to avoid loss"
        )
        try:
            fn()
        except Exception:  # pragma: no cover - defensive fallback
            _log.exception("async task failed during synchronous fallback")
