Folgende Pakete müssen installiert werden: 

kivy	
pandas
numpy	
opencv-contrib-python
sounddevice


pip install kivy pandas numpy opencv-contrib-python sounddevice

## Event-Synchronisation

Die Tabletop-App sendet pro Ereignis ausschließlich eine minimierte Payload.
Erlaubt sind die Felder `session`, `block`, `player`, `button`, `phase`,
`round_index`, `game_player`, `player_role`, `accepted`, `decision` und
`actor`. Weitere Metadaten wie `event_id`, `mapping_version`, `origin_device`
oder Queue-/Heartbeat-Informationen werden nicht mehr erzeugt oder übertragen.

Ein dediziertes Sync-Event (`sync.block.pre`) informiert die Geräte genau einmal
vor dem Start eines neuen Blocks über die kommenden Block- und Session-IDs.
Laufende Heartbeat- und Host-Sync-Schleifen entfallen vollständig.

Einen schnellen Smoke-Test liefert:

```bash
python -m tabletop.app --demo
```

Der Demo-Lauf simuliert UI-Events mit der gleichen Whitelisting-Logik und
gibt die gesendeten Payloads in der Konsole aus.

## Neon Companion Hinweise

- Die Companion-API stellt keinen dedizierten Capabilities-Endpunkt mehr bereit. Geräteeigenschaften
  werden ausschließlich über die Status-Websocket-Payloads bestimmt.
- `device_id` ist optional – fällt sie weg, nutzt die Bridge automatisch den
  `ip:port`-Endpunkt als Schlüssel und protokolliert den Fallback.
- Die Zeit-Synchronisation verwendet `estimate_time_offset()` der offiziellen
  Realtime-API. Pro Gerät wird einmalig der Offset `clock_offset_ns =
  round(estimate.time_offset_ms.mean * 1_000_000)` bestimmt und bei Events als
  `event_timestamp_unix_ns = time.time_ns() - clock_offset_ns` angewandt.
- Gelingt die Messung nicht, bricht der Startvorgang hart ab – es existiert kein
  Fallback auf unkalibrierte Laptop-Zeit.
- Alle älteren Sync-Mechanismen (TimeReconciler, Marker-Refines, Soft-Offsets)
  wurden entfernt, damit der Ablauf klar und nachvollziehbar bleibt.
