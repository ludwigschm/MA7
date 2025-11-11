"""Shared HTTP client helpers for synchronous and asynchronous usage."""

from __future__ import annotations

import threading
from typing import Optional

import httpx
import requests
from requests import Session
from requests.adapters import HTTPAdapter

_ASYNC_CLIENT: Optional[httpx.AsyncClient] = None
_ASYNC_LOCK = threading.Lock()

_SYNC_SESSION: Optional[Session] = None
_SYNC_LOCK = threading.Lock()


class _TimeoutHTTPAdapter(HTTPAdapter):
    """HTTP adapter that injects default timeouts when not provided."""

    def __init__(self, *args: object, timeout: float | tuple[float, float] = 0.5, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._timeout = timeout

    def send(self, request, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._timeout)
        return super().send(request, **kwargs)


def get_async_client() -> httpx.AsyncClient:
    """Return the shared :class:`httpx.AsyncClient` instance."""

    global _ASYNC_CLIENT
    if _ASYNC_CLIENT is None:
        with _ASYNC_LOCK:
            if _ASYNC_CLIENT is None:
                _ASYNC_CLIENT = httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=0.5, read=0.5, write=0.5, pool=1.0),
                    limits=httpx.Limits(
                        max_keepalive_connections=8,
                        max_connections=8,
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
                    pool_connections=8,
                    pool_maxsize=8,
                    max_retries=0,
                    timeout=(0.5, 0.5),
                )
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                session.headers.setdefault("Connection", "keep-alive")
                _SYNC_SESSION = session
    return _SYNC_SESSION

