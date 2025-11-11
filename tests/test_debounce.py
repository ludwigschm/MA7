import time

from core.event_router import EventRouter, UIEvent


def test_debounce_coalesces_quick_tap_events() -> None:
    delivered: list[tuple[str, UIEvent]] = []

    def deliver(player: str, event: UIEvent) -> None:
        delivered.append((player, event))

    router = EventRouter(deliver, normal_batch_interval_s=0.001, normal_max_batch=8)
    router.register_player("VP1")
    router.set_active_player("VP1")

    for seq in range(5):
        router.route(UIEvent(name="tap.card", target="VP1", payload={"seq": seq}))
        time.sleep(0.002)

    time.sleep(0.05)
    router.flush_all()

    assert len(delivered) == 1
    player, event = delivered[0]
    assert player == "VP1"
    assert event.payload == {"seq": 4}
    assert router.events_coalesced_total == 4
