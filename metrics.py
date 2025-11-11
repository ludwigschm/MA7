"""Minimal metrics shim for optional instrumentation backends."""

from __future__ import annotations

import logging
from typing import Any, Protocol

log = logging.getLogger("metrics")


class MetricsBackend(Protocol):
    """Protocol describing the minimal metrics backend interface."""

    def inc(self, name: str, **labels: Any) -> None: ...

    def observe(self, name: str, value: float, **labels: Any) -> None: ...

    def gauge(self, name: str, value: float, **labels: Any) -> None: ...


_backend: MetricsBackend | None = None


def configure(backend: MetricsBackend | None) -> None:
    """Configure the metrics backend to use for instrumentation."""

    global _backend
    _backend = backend


def _debug_log(action: str, name: str, value: float | None, labels: dict[str, Any]) -> None:
    if log.isEnabledFor(logging.DEBUG):
        if value is None:
            log.debug("metric %s %s labels=%s", action, name, labels)
        else:
            log.debug("metric %s %s value=%s labels=%s", action, name, value, labels)


def inc(name: str, **labels: Any) -> None:
    """Increment a counter."""

    if _backend is not None:
        try:
            _backend.inc(name, **labels)
            return
        except Exception:  # pragma: no cover - backend defined externally
            log.exception("metrics backend inc failed name=%s", name)
    _debug_log("inc", name, None, labels)


def observe(name: str, value: float, **labels: Any) -> None:
    """Record a histogram/summary observation."""

    if _backend is not None:
        try:
            _backend.observe(name, value, **labels)
            return
        except Exception:  # pragma: no cover - backend defined externally
            log.exception("metrics backend observe failed name=%s", name)
    _debug_log("observe", name, value, labels)


def gauge(name: str, value: float, **labels: Any) -> None:
    """Set a gauge value."""

    if _backend is not None:
        try:
            _backend.gauge(name, value, **labels)
            return
        except Exception:  # pragma: no cover - backend defined externally
            log.exception("metrics backend gauge failed name=%s", name)
    _debug_log("gauge", name, value, labels)
