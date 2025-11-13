import queue
import time

from core.event_router import EventRouter, UIEvent


def test_fix_events_emit_immediately() -> None:
    calls: "queue.Queue[tuple[str, str, float]]" = queue.Queue()

    def deliver(player: str, event: UIEvent) -> None:
        calls.put((player, event.name, time.perf_counter()))

    router = EventRouter(deliver)
    router.register_player("VP1")
    router.set_active_player("VP1")

    start = time.perf_counter()
    router.route(UIEvent(name="fix.cross", target="VP1"))
    player, name, ts = calls.get(timeout=0.1)

    assert player == "VP1"
    assert name == "fix.cross"
    assert ts - start < 0.01
    assert router.events_high_total == 1

    router.flush_all()


def test_normal_events_batch_and_high_not_delayed() -> None:
    calls: "queue.Queue[tuple[str, str, float]]" = queue.Queue()

    def deliver(player: str, event: UIEvent) -> None:
        calls.put((player, event.name, time.perf_counter()))

    router = EventRouter(deliver, normal_batch_interval_s=0.005, normal_max_batch=4)
    router.register_player("VP1")
    router.set_active_player("VP1")

    router.route(UIEvent(name="normal.a", target="VP1"))
    router.route(UIEvent(name="normal.b", target="VP1"))

    time.sleep(0.001)
    assert calls.empty()

    start = time.perf_counter()
    router.route(UIEvent(name="fix.now", target="VP1"))

    player, name, ts = calls.get(timeout=0.1)
    assert (player, name) == ("VP1", "fix.now")
    assert ts - start < 0.01

    names = [calls.get(timeout=0.1)[1] for _ in range(2)]
    assert sorted(names) == ["normal.a", "normal.b"]
    assert router.normal_batches_total == 1
    assert router.events_normal_total == 2

    router.flush_all()
