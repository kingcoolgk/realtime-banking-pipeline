"""
consumer/transformations.py
-----------------------------
Business transformation and feature engineering logic
applied to each micro-batch before DQ and storage.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, udf, round as spark_round
)
from pyspark.sql.types import StringType, DoubleType


# ── Risk Score UDF ────────────────────────────────────────────────────────────
def compute_risk_score(amount: float, is_international: bool,
                        status: str, merchant_category: str) -> float:
    """
    Heuristic risk scoring (0.0 - 1.0).
    Production version would use a trained ML model.
    """
    score = 0.0
    if amount and amount > 50000:
        score += 0.4
    elif amount and amount > 20000:
        score += 0.2
    if is_international:
        score += 0.3
    if status == "FAILED":
        score += 0.2
    if merchant_category in ("TRAVEL", "ECOMMERCE"):
        score += 0.1
    return min(round(score, 2), 1.0)


risk_score_udf = udf(compute_risk_score, DoubleType())


# ── Merchant Category Normaliser ──────────────────────────────────────────────
CATEGORY_MAP = {
    "GROCERY":       "ESSENTIAL",
    "UTILITIES":     "ESSENTIAL",
    "HEALTHCARE":    "ESSENTIAL",
    "FUEL":          "TRANSPORT",
    "TRAVEL":        "TRANSPORT",
    "RESTAURANT":    "LIFESTYLE",
    "ENTERTAINMENT": "LIFESTYLE",
    "ECOMMERCE":     "DIGITAL",
}


def normalise_category(cat: str) -> str:
    return CATEGORY_MAP.get(cat, "OTHER") if cat else "OTHER"


normalise_cat_udf = udf(normalise_category, StringType())


# ── Main Transformation Function ──────────────────────────────────────────────
def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply all enrichments and derived columns to the raw parsed DataFrame.

    Columns added:
        - risk_score          : float 0–1 heuristic fraud risk
        - risk_label          : LOW / MEDIUM / HIGH
        - category_group      : normalised merchant category bucket
        - amount_bucket       : binned transaction amount range
        - is_high_value       : boolean flag for amounts > 50,000 INR
    """

    df = df.withColumn(
        "risk_score",
        risk_score_udf(
            col("amount"),
            col("is_international"),
            col("status"),
            col("merchant_category")
        )
    )

    df = df.withColumn(
        "risk_label",
        when(col("risk_score") >= 0.7, lit("HIGH"))
        .when(col("risk_score") >= 0.4, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    df = df.withColumn(
        "category_group",
        normalise_cat_udf(col("merchant_category"))
    )

    df = df.withColumn(
        "amount_bucket",
        when(col("amount") < 500,   lit("MICRO"))
        .when(col("amount") < 5000,  lit("SMALL"))
        .when(col("amount") < 20000, lit("MEDIUM"))
        .when(col("amount") < 50000, lit("LARGE"))
        .otherwise(lit("HIGH_VALUE"))
    )

    df = df.withColumn(
        "is_high_value",
        col("amount") > 50000
    )

    df = df.withColumn(
        "amount",
        spark_round(col("amount"), 2)
    )

    return df
