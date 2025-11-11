from core.capabilities import CapabilityRegistry, DeviceCapabilities


def test_caps_cached_and_mutable():
    registry = CapabilityRegistry()
    caps = registry.get("devA")
    assert caps.supports_frame_name is False

    updated = DeviceCapabilities(supports_frame_name=True)
    registry.set("devA", updated)
    cached = registry.get("devA")
    assert cached.supports_frame_name is True
    assert registry.get("devB").supports_frame_name is False
