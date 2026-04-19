"""
storage/s3_writer.py
----------------------
Writes processed DataFrames to local filesystem (demo mode).
In production: swap local paths for s3a:// paths.
"""

import logging
import os
import yaml

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

RAW_PREFIX     = CONFIG["s3"]["raw_prefix"]
CURATED_PREFIX = CONFIG["s3"]["curated_prefix"]
DQ_PREFIX      = CONFIG["s3"]["dq_prefix"]

# Create output directories if they don't exist
os.makedirs(RAW_PREFIX,     exist_ok=True)
os.makedirs(CURATED_PREFIX, exist_ok=True)
os.makedirs(DQ_PREFIX,      exist_ok=True)


def write_to_raw_zone(df: DataFrame, batch_id: int):
    """
    Write all incoming records to local raw zone as Parquet.
    Partitioned by event_date and card_type.
    """
    try:
        (
            df.write
            .mode("append")
            .partitionBy("event_date", "card_type")
            .parquet(RAW_PREFIX)
        )
        logger.info(f"Batch {batch_id} | Raw zone write SUCCESS → {RAW_PREFIX}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Raw zone write FAILED: {e}")
        raise


def write_to_curated_zone(df: DataFrame, batch_id: int):
    """
    Write DQ-passed deduplicated records to local curated zone.
    Partitioned by event_date and category_group.
    """
    deduped_df = df.dropDuplicates(["transaction_id"])

    try:
        (
            deduped_df.write
            .mode("append")
            .partitionBy("event_date", "category_group")
            .parquet(CURATED_PREFIX)
        )
        logger.info(f"Batch {batch_id} | Curated zone write SUCCESS → {CURATED_PREFIX}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Curated zone write FAILED: {e}")
        raise


def write_dq_report(report: dict, batch_id: int, spark):
    """
    Persist DQ report as Parquet for audit trail.
    """
    import json
    rdd   = spark.sparkContext.parallelize([json.dumps(report)])
    dq_df = spark.read.json(rdd)

    try:
        dq_df.write.mode("append").parquet(DQ_PREFIX)
        logger.info(f"Batch {batch_id} | DQ report written → {DQ_PREFIX}")
    except Exception as e:
        logger.warning(f"Batch {batch_id} | DQ report write failed: {e}")