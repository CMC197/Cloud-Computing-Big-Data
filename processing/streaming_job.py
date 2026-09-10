"""AP2 — Spark Structured Streaming Job.
Kafka (Source) -> Watermark/Windowing -> Delta auf MinIO (Bronze + Gold + bay_current).
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "smartpark-kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "parking-events")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("BUCKET", "smartpark-lakehouse")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/parking_events"
GOLD_PATH = f"s3a://{BUCKET}/gold/zone_availability"
BAY_CURRENT_PATH = f"s3a://{BUCKET}/gold/bay_current"

schema = StructType([
    StructField("event_id", StringType()),
    StructField("bay_id", StringType()),
    StructField("zone_id", StringType()),
    StructField("state", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
])


def build_spark():
    return (
        SparkSession.builder.appName("smartpark-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )


def upsert_bay_current(batch_df, batch_id):
    """Schreibt pro bay_id das juengste Event dieses Batches als aktuellen
    Zustand nach gold/bay_current (Delta-Merge / Upsert)."""
    if batch_df.rdd.isEmpty():
        return
    w = Window.partitionBy("bay_id").orderBy(col("event_ts").desc())
    latest = (
        batch_df.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("bay_id", "zone_id", "state", "event_ts")
    )
    spark = batch_df.sparkSession
    if DeltaTable.isDeltaTable(spark, BAY_CURRENT_PATH):
        tgt = DeltaTable.forPath(spark, BAY_CURRENT_PATH)
        (
            tgt.alias("t")
            .merge(latest.alias("s"), "t.bay_id = s.bay_id")
            .whenMatchedUpdateAll(condition="s.event_ts > t.event_ts")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        latest.write.format("delta").mode("overwrite").save(BAY_CURRENT_PATH)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("Streaming gestartet: " + KAFKA_BROKERS + "/" + KAFKA_TOPIC + " -> " + BUCKET)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("e"))
        .select("e.*")
        .withWatermark("event_ts", WATERMARK_DELAY)
    )

    bronze_query = (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR + "/bronze")
        .start(BRONZE_PATH)
    )

    gold = (
        events.groupBy(window(col("event_ts"), WINDOW_DURATION), col("zone_id"))
        .agg(
            count("*").alias("events_total"),
            _sum(when(col("state") == "OCCUPIED", 1).otherwise(0)).alias("occupied"),
            _sum(when(col("state") == "FREE", 1).otherwise(0)).alias("free"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone_id"),
            col("events_total"),
            col("occupied"),
            col("free"),
        )
    )

    gold_query = (
        gold.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR + "/gold")
        .start(GOLD_PATH)
    )

    bay_current_query = (
        events.writeStream
        .foreachBatch(upsert_bay_current)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR + "/bay_current")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
