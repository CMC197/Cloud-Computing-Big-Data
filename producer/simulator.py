"""AP1 — Event-Simulator.

Erzeugt synthetische Belegungs-Events (FREE <-> OCCUPIED) je Parkbucht und
schreibt sie nach Kafka. Skaliert horizontal ueber Replicas.

TODO(AP1):
  - Konfiguration aus ENV lesen (KAFKA_BROKERS, KAFKA_TOPIC, EVENT_RATE)
  - Realistische Verteilungen: Tages-/Stosszeiten, Verweildauern
  - Key = zone_id, damit Kafka nach Zone partitioniert
"""


def main() -> None:
    raise NotImplementedError("AP1: siehe Checkliste Phase 1")


if __name__ == "__main__":
    main()
