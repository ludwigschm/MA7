"""Compatibility package exposing canonical modules under the ``MA7_main`` namespace."""

from importlib import import_module as _import_module
import sys as _sys

_core_mod = _import_module("core")
_sys.modules.setdefault(__name__ + ".core", _core_mod)
_sys.modules.setdefault(__name__ + ".core.clock", _import_module("core.clock"))
_sys.modules.setdefault(__name__ + ".core.time_sync", _import_module("core.time_sync"))

core = _core_mod

__all__ = ["core"]
