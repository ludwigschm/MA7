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
