"""
storage/s3_writer.py
----------------------
Writes processed DataFrames to the S3 Lakehouse.

Zones:
  - Raw Zone    : every micro-batch, unfiltered, Parquet, partitioned by event_date + card_type
  - Curated Zone: DQ-passed records only, deduplicated, partition-optimised for BI queries
"""

import logging
import yaml

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

S3_BUCKET       = CONFIG["s3"]["bucket"]
RAW_PREFIX      = CONFIG["s3"]["raw_prefix"]
CURATED_PREFIX  = CONFIG["s3"]["curated_prefix"]
DQ_PREFIX       = CONFIG["s3"]["dq_prefix"]


def write_to_raw_zone(df: DataFrame, batch_id: int):
    """
    Write all incoming records to the raw zone as an audit trail.
    Partitioned by event_date and card_type for efficient time-range scans.
    """
    raw_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}"

    try:
        (
            df.write
            .mode("append")
            .partitionBy("event_date", "card_type")
            .parquet(raw_path)
        )
        logger.info(f"Batch {batch_id} | Raw zone write SUCCESS → {raw_path}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Raw zone write FAILED: {e}")
        raise


def write_to_curated_zone(df: DataFrame, batch_id: int):
    """
    Write DQ-passed, deduplicated records to the curated zone.
    Deduplication on transaction_id ensures idempotent micro-batch writes.
    Partitioned by event_date + category_group for BI query optimisation.
    """
    curated_path = f"s3a://{S3_BUCKET}/{CURATED_PREFIX}"

    # Deduplicate within the batch on transaction_id
    deduped_df = df.dropDuplicates(["transaction_id"])

    try:
        (
            deduped_df.write
            .mode("append")
            .partitionBy("event_date", "category_group")
            .parquet(curated_path)
        )
        logger.info(f"Batch {batch_id} | Curated zone write SUCCESS → {curated_path}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Curated zone write FAILED: {e}")
        raise


def write_dq_report(report: dict, batch_id: int, spark):
    """
    Persist the DQ report dict as a single-row JSON Parquet for audit/monitoring.
    """
    import json
    dq_path  = f"s3a://{S3_BUCKET}/{DQ_PREFIX}"
    rdd      = spark.sparkContext.parallelize([json.dumps(report)])
    dq_df    = spark.read.json(rdd)

    try:
        dq_df.write.mode("append").parquet(dq_path)
        logger.info(f"Batch {batch_id} | DQ report written → {dq_path}")
    except Exception as e:
        logger.warning(f"Batch {batch_id} | DQ report write failed (non-critical): {e}")
