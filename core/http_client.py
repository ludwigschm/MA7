"""Shared HTTP client helpers for synchronous and asynchronous usage."""

from __future__ import annotations

import logging
import threading
from typing import Optional

import requests
from requests import Session
from requests.adapters import HTTPAdapter

try:  # pragma: no cover - optional dependency
    import httpx  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
    httpx = None  # type: ignore[assignment]
    _HTTPX_IMPORT_ERROR = exc
else:  # pragma: no cover - optional dependency
    _HTTPX_IMPORT_ERROR = None

from .config import HTTP_CONNECT_TIMEOUT_S, HTTP_MAX_CONNECTIONS

log = logging.getLogger(__name__)

if httpx is None:  # pragma: no cover - optional dependency
    log.warning(
        "httpx is not installed; asynchronous HTTP support is disabled. "
        "Install the 'httpx' extra to enable async APIs.",
    )

_ASYNC_CLIENT: Optional["httpx.AsyncClient"] = None
_ASYNC_LOCK = threading.Lock()

_SYNC_SESSION: Optional[Session] = None
_SYNC_LOCK = threading.Lock()


class _TimeoutHTTPAdapter(HTTPAdapter):
    """HTTP adapter that injects default timeouts when not provided."""

    def __init__(
        self,
        *args: object,
        timeout: float | tuple[float, float] = HTTP_CONNECT_TIMEOUT_S,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._timeout = timeout

    def send(self, request, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._timeout)
        return super().send(request, **kwargs)


def _ensure_async_support() -> None:
    """Validate that the optional :mod:`httpx` dependency is available."""

    if httpx is None:
        message = (
            "httpx is not available; install the 'httpx' extra to enable async HTTP "
            "operations"
        )
        if _HTTPX_IMPORT_ERROR is not None:
            raise RuntimeError(message) from _HTTPX_IMPORT_ERROR
        raise RuntimeError(message)


def get_async_client() -> "httpx.AsyncClient":
    """Return the shared :class:`httpx.AsyncClient` instance."""

    _ensure_async_support()

    global _ASYNC_CLIENT
    if _ASYNC_CLIENT is None:
        with _ASYNC_LOCK:
            if _ASYNC_CLIENT is None:
                assert httpx is not None  # Narrow type for mypy/pyright
                _ASYNC_CLIENT = httpx.AsyncClient(
                    timeout=httpx.Timeout(
                        connect=HTTP_CONNECT_TIMEOUT_S,
                        read=HTTP_CONNECT_TIMEOUT_S,
                        write=HTTP_CONNECT_TIMEOUT_S,
                        pool=1.0,
                    ),
                    limits=httpx.Limits(
                        max_keepalive_connections=HTTP_MAX_CONNECTIONS,
                        max_connections=HTTP_MAX_CONNECTIONS,
                        keepalive_expiry=30.0,
                    ),
                    http2=True,
                )
    return _ASYNC_CLIENT


def get_sync_session() -> Session:
    """Return the shared :class:`requests.Session` instance."""

    global _SYNC_SESSION
    if _SYNC_SESSION is None:
        with _SYNC_LOCK:
            if _SYNC_SESSION is None:
                session = requests.Session()
                adapter = _TimeoutHTTPAdapter(
                    pool_connections=HTTP_MAX_CONNECTIONS,
                    pool_maxsize=HTTP_MAX_CONNECTIONS,
                    max_retries=0,
                    timeout=(HTTP_CONNECT_TIMEOUT_S, HTTP_CONNECT_TIMEOUT_S),
                )
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                session.headers.setdefault("Connection", "keep-alive")
                _SYNC_SESSION = session
    return _SYNC_SESSION

