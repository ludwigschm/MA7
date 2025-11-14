# Latency & Sync Leitfaden

## Prioritäten
- Verwende `priority="high"` ausschließlich für `fix.*`-Events. Das einzige `sync.*`-Event (`sync.block.pre`) läuft mit niedriger Priorität, da es nur einmal vor Blockstart gesendet wird.
- Normale Events laufen über die Batch-Queue (`priority="normal"`). Sie profitieren vom reduzierten Fenster (`~5 ms`) und der Batch-Größe (4 Events).

## Sync-Strategie
- Herzschlag- und Host-Syncs sind deaktiviert. Geräte erhalten einmalig vor jedem Block ein `sync.block.pre` Event mit Session- und Block-ID.
- Die verbleibenden `fix.*`-Marker folgen unverändert dem High-Priority-Pfad.

## Clock-Offset
- Die Bridge nutzt `device.estimate_time_offset()` der offiziellen Realtime-API, um pro Gerät genau einmal den Offset zu bestimmen.
- Schlägt die Messung fehl (Gerät fehlt, API-Fehler, Timeout), bricht der Startvorgang hart mit einer Exception ab – Experimente laufen nie ohne Offset.
- Der Offset wird als `clock_offset_ns = round(estimate.time_offset_ms.mean * 1_000_000)` gespeichert und über `event_timestamp_unix_ns = time.time_ns() - clock_offset_ns` auf jedes Event angewandt.
- Legacy-Komponenten wie TimeReconciler, Marker-Refines oder Soft-Offsets wurden vollständig entfernt, damit der Pfad klar und wartbar bleibt.

## Batch-Parameter anpassen
- Standardwerte: Fenster `5 ms`, Batch-Größe `4`.
- Umgebungsvariablen:
  - `EVENT_BATCH_WINDOW_MS` – neues Fenster in Millisekunden.
  - `EVENT_BATCH_SIZE` – neue Batch-Größe (Minimum 1).
- `LOW_LATENCY_DISABLED=1` deaktiviert die Queue komplett (alle Events werden synchron gesendet).
- `PERF_LOGGING=1` aktiviert Latenzlogs mit `t_ui_ns`, `t_enqueue_ns` und `t_dispatch_ns`.
