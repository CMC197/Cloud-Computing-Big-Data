"""AP2 — Spark Structured Streaming Job.

Kafka (Source) -> Transformationen -> Delta auf MinIO (Bronze + Gold).

TODO(AP2), siehe Checkliste Phase 2:
  - readStream.format("kafka"), Broker aus ConfigMap
  - Transformation 1: Windowed Aggregation je zone_id
  - Transformation 2: Stateful Session-Join (Verweildauer je bay_id)
  - Transformation 3: Enrichment via Broadcast-Join mit Zonen-Stammdaten
  - .withWatermark("event_ts", "2 minutes"), Late-Data-Strategie
  - Checkpointing auf PVC /checkpoints/
"""


def main() -> None:
    raise NotImplementedError("AP2: siehe Checkliste Phase 2")


if __name__ == "__main__":
    main()
