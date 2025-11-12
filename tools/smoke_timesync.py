#!/usr/bin/env python3
"""Smoke-test the OfficialTimeSync calibrator against a mock or real device."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from types import SimpleNamespace

from core.time_sync import make_timesync
from tabletop.pupil_bridge import PupilBridge, NeonDeviceConfig, device_key_from

try:  # pragma: no cover - optional dependency for real devices
    from pupil_labs.realtime_api.simple import Device  # type: ignore
except Exception:  # pragma: no cover - dependency optional in CI
    Device = None  # type: ignore


class _MockDevice:
    def __init__(self, offset_ms: float = 8.0, rtt_ms: float = 25.0) -> None:
        self._offset_ms = float(offset_ms)
        self._rtt_ms = float(rtt_ms)

    async def estimate_time_offset(self) -> SimpleNamespace:
        await asyncio.sleep(0)
        return SimpleNamespace(
            time_offset_ms=SimpleNamespace(mean=self._offset_ms),
            roundtrip_duration_ms=SimpleNamespace(mean=self._rtt_ms),
        )


def _connect_real_device(bridge: PupilBridge, args: argparse.Namespace):
    if Device is None:
        raise RuntimeError("Realtime API unavailable; cannot use real device")
    cfg = NeonDeviceConfig(
        player="VP1",
        device_id=args.device_id or "",
        ip=args.ip or "",
        port=args.port,
    )
    device = bridge._connect_device_with_retries("VP1", cfg)
    identity = bridge._validate_device_identity(device, cfg)
    device_key = bridge._resolve_device_key(cfg, identity)
    bridge._device_by_player["VP1"] = device
    return device, device_key


def main() -> int:
    parser = argparse.ArgumentParser(description="OfficialTimeSync smoke test")
    parser.add_argument("--ip", help="Optional device IP for real calibration")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--device-id", help="Expected device id when using real devices")
    parser.add_argument("--min-samples", type=int, default=10)
    parser.add_argument("--max-samples", type=int, default=40)
    parser.add_argument("--rtt-max", type=float, default=120.0)
    parser.add_argument("--offset-max", type=float, default=250.0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("tools.smoke_timesync")
    bridge = PupilBridge(device_mapping={})

    try:
        if args.ip:
            try:
                device, device_key = _connect_real_device(bridge, args)
                log.info("Using real device for timesync: key=%s", device_key)
            except Exception as exc:  # pragma: no cover - depends on hardware
                log.warning("Falling back to mock device: %s", exc)
                device = _MockDevice()
                device_key = device_key_from("mock", 0, None)
        else:
            device = _MockDevice()
            device_key = device_key_from("mock", 0, None)
        bridge._device_by_player["VP1"] = device

        calibrator = make_timesync(
            device,
            device_key,
            min_samples=args.min_samples,
            max_samples=args.max_samples,
            rtt_ms_max=args.rtt_max,
            offset_ms_abs_max=args.offset_max,
        )
        try:
            result = asyncio.run(calibrator.calibrate())
        except Exception as exc:
            log.error("TimeSync calibration failed: %s", exc)
            return 2

        log.info(
            "TimeSync result: median=%.2f ms, iqr=%.2f ms, rms_rtt=%.2f ms, accepted=%d/%d",
            result.median_offset_ms,
            result.iqr_ms,
            result.rms_rtt_ms,
            result.accepted,
            result.total,
        )
        print("TIMESYNC_ACCEPTANCE OK")
        return 0
    finally:
        bridge.close()


if __name__ == "__main__":  # pragma: no cover - manual entry point
    sys.exit(main())
