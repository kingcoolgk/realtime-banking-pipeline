"""
producer/transaction_schema.py
--------------------------------
Canonical schema for credit card transaction events.
Used for schema drift detection in the DQ layer.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    nullable=False),
    StructField("card_id",           StringType(),    nullable=False),
    StructField("customer_id",       StringType(),    nullable=False),
    StructField("amount",            DoubleType(),    nullable=False),
    StructField("currency",          StringType(),    nullable=True),
    StructField("card_type",         StringType(),    nullable=True),
    StructField("merchant_name",     StringType(),    nullable=True),
    StructField("merchant_category", StringType(),    nullable=True),
    StructField("city",              StringType(),    nullable=True),
    StructField("status",            StringType(),    nullable=True),
    StructField("is_international",  BooleanType(),   nullable=True),
    StructField("event_timestamp",   StringType(),    nullable=False),
    StructField("event_date",        StringType(),    nullable=False),
])

# Critical fields — null rate must stay below DQ threshold
CRITICAL_FIELDS = ["transaction_id", "card_id", "amount", "event_timestamp", "event_date"]

# Schema field names for drift comparison
EXPECTED_FIELDS = set(f.name for f in TRANSACTION_SCHEMA.fields)
