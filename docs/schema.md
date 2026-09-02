# Datenschema — SmartPark Lakehouse

## Bronze — s3a://smartpark-lakehouse/bronze/parking_events
Rohe Belegungs-Events, 1:1 aus Kafka, unverändert. Append-only.

| Feld       | Typ       | Beschreibung                                  |
|------------|-----------|-----------------------------------------------|
| event_id   | String    | Eindeutige Event-ID (UUID)                    |
| bay_id     | String    | ID der Parkbucht (z.B. bay-2-7)               |
| zone_id    | String    | ID der Zone (z.B. zone-2) — Kafka-Partition   |
| state      | String    | FREE oder OCCUPIED                            |
| event_ts   | Timestamp | Zeitpunkt des Ereignisses (Event-Time)        |
| ingest_ts  | Timestamp | Zeitpunkt der Erzeugung im Producer           |

Format: Delta. Watermark auf event_ts (2 min).

## Gold — s3a://smartpark-lakehouse/gold/zone_availability
Aggregierte Verfügbarkeit je Zone und 1-Minuten-Fenster.

| Feld          | Typ       | Beschreibung                        |
|---------------|-----------|-------------------------------------|
| window_start  | Timestamp | Beginn des 1-Minuten-Fensters       |
| window_end    | Timestamp | Ende des Fensters                   |
| zone_id       | String    | ID der Zone                         |
| events_total  | Long      | Anzahl Events im Fenster            |
| occupied      | Long      | Anzahl OCCUPIED-Events im Fenster   |
| free          | Long      | Anzahl FREE-Events im Fenster       |

Format: Delta. Aggregation via Structured Streaming (groupBy window + zone_id).

## Begründung (für README §6)
- Delta statt reines Parquet: ACID, Schema-Enforcement, Zeitreise, sichere concurrent Writes.
- Bronze/Gold-Trennung (Medaillon): Rohdaten reprozessierbar, Gold abfrageoptimiert.
- Partitionierung nach zone_id/Zeit durch Windowing.
- MinIO als S3-kompatibler Objektspeicher: Storage von Compute entkoppelt, on-prem auf DHBWCloud.
