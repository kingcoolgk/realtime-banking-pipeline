"""
consumer/stream_consumer.py
-----------------------------
PySpark Structured Streaming consumer.
Reads from Kafka topic, applies transformations,
runs DQ checks, and writes to S3 Lakehouse.

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
      consumer/stream_consumer.py
"""

import sys
import yaml
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp
)

sys.path.append(".")
from producer.transaction_schema import TRANSACTION_SCHEMA
from consumer.transformations import apply_transformations
from dq.dq_checks import run_dq_checks
from storage.s3_writer import write_to_raw_zone, write_to_curated_zone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# ── Load config ────────────────────────────────────────────────────────────────
with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

KAFKA_BROKER     = CONFIG["kafka"]["broker"]
TOPIC            = CONFIG["kafka"]["topic"]
CHECKPOINT_PATH  = CONFIG["spark"]["checkpoint_path"]
TRIGGER_INTERVAL = CONFIG["spark"]["trigger_interval"]   # e.g. "30 seconds"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RealtimeBankingPipeline")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def process_batch(batch_df, batch_id: int):
    """
    Micro-batch processing function called by Spark for each trigger interval.
    Steps: Parse → Transform → DQ → Write Raw → Write Curated (if DQ passed)
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty — skipping.")
        return

    count = batch_df.count()
    logger.info(f"Batch {batch_id}: received {count} records.")

    # 1. Apply business transformations & enrichments
    transformed_df = apply_transformations(batch_df)

    # 2. Always write raw zone (unfiltered audit trail)
    write_to_raw_zone(transformed_df, batch_id)

    # 3. Run DQ checks — returns (passed_df, dq_report_dict)
    passed_df, dq_report = run_dq_checks(transformed_df, batch_id)

    if dq_report["batch_passed"]:
        # 4. Write DQ-passed records to curated zone
        write_to_curated_zone(passed_df, batch_id)
        logger.info(f"Batch {batch_id}: DQ passed. {passed_df.count()} records → curated zone.")
    else:
        logger.warning(
            f"Batch {batch_id}: DQ FAILED. "
            f"Failures: {dq_report['failures']}. Records quarantined."
        )


def run_streaming():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Connecting to Kafka broker: {KAFKA_BROKER} | Topic: {TOPIC}")

    # ── Read from Kafka ────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)   # backpressure control
        .load()
    )

    # ── Deserialize JSON payload ───────────────────────────────────────────────
    parsed_stream = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("ingestion_timestamp", current_timestamp())
        # Watermark to handle late-arriving events up to 5 minutes
        .withWatermark("event_timestamp", "5 minutes")
    )

    # ── Write stream using foreachBatch ───────────────────────────────────────
    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming()
