#!/usr/bin/env python3
"""
PySpark batch job for Task 2.

Reads applications CSV from Object Storage or local filesystem,
casts columns to proper types, prepares cleaned dataset and analytical aggregates,
then writes results as Parquet.

Local run example:
  spark-submit spark/batch/process_applications.py \
    --input data/generated/applications.csv \
    --output data/output/task_02/applications

Cloud run example:
  spark-submit process_applications.py \
    --input s3a://<bucket>/etl/task_02/input/applications.csv \
    --output s3a://<bucket>/etl/task_02/output/applications
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process applications CSV with PySpark")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Base output path")
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("etl-module4-applications-batch")
        .getOrCreate()
    )


def get_schema() -> StructType:
    return StructType(
        [
            StructField("application_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region_code", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("requested_amount", DoubleType(), True),
            StructField("term_months", IntegerType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
            StructField("decision_status", StringType(), True),
            StructField("approved_amount", DoubleType(), True),
            StructField("channel", StringType(), True),
            StructField("employee_review_flag", BooleanType(), True),
            StructField("processing_time_sec", DoubleType(), True),
        ]
    )


def add_technical_columns(df):
    return (
        df
        .withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_month", F.date_format("event_timestamp", "yyyy-MM"))
        .withColumn(
            "is_approved",
            F.when(F.col("decision_status") == F.lit("approved"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_rejected",
            F.when(F.col("decision_status") == F.lit("rejected"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_manual_review",
            F.when(F.col("decision_status") == F.lit("manual_review"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("approved_amount", F.coalesce(F.col("approved_amount"), F.lit(0.0)))
        .withColumn("requested_amount", F.coalesce(F.col("requested_amount"), F.lit(0.0)))
        .withColumn("processing_time_sec", F.coalesce(F.col("processing_time_sec"), F.lit(0.0)))
    )


def with_approval_rate(df):
    return df.withColumn(
        "approval_rate",
        F.round(F.col("approved_count") / F.col("applications_count"), 4),
    )


def main() -> None:
    args = parse_args()
    spark = build_spark_session()

    df_raw = (
        spark.read
        .option("header", "true")
        .schema(get_schema())
        .csv(args.input)
    )

    df_cleaned = add_technical_columns(df_raw)

    (
        df_cleaned
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/applications_cleaned")
    )

    agg_by_region_product = (
        df_cleaned
        .groupBy("region_code", "product_type", "risk_level")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("is_approved").alias("approved_count"),
            F.sum("is_rejected").alias("rejected_count"),
            F.sum("is_manual_review").alias("manual_review_count"),
            F.round(F.sum("requested_amount"), 2).alias("requested_amount_total"),
            F.round(F.sum("approved_amount"), 2).alias("approved_amount_total"),
            F.round(F.avg("requested_amount"), 2).alias("avg_requested_amount"),
            F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score"),
            F.round(F.avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        )
    )

    (
        with_approval_rate(agg_by_region_product)
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/agg_by_region_product")
    )

    agg_by_day = (
        df_cleaned
        .groupBy("event_date")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("is_approved").alias("approved_count"),
            F.sum("is_rejected").alias("rejected_count"),
            F.sum("is_manual_review").alias("manual_review_count"),
            F.round(F.sum("requested_amount"), 2).alias("requested_amount_total"),
            F.round(F.sum("approved_amount"), 2).alias("approved_amount_total"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score"),
            F.round(F.avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        )
        .orderBy("event_date")
    )

    (
        with_approval_rate(agg_by_day)
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/agg_by_day")
    )

    # 4. Aggregation by application channel
    agg_by_channel = (
        df_cleaned
        .groupBy("channel", "employee_review_flag")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("is_approved").alias("approved_count"),
            F.sum("is_rejected").alias("rejected_count"),
            F.sum("is_manual_review").alias("manual_review_count"),
            F.round(F.avg("requested_amount"), 2).alias("avg_requested_amount"),
            F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
            F.round(F.avg("credit_score"), 2).alias("avg_credit_score"),
            F.round(F.avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        )
    )

    (
        with_approval_rate(agg_by_channel)
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/agg_by_channel")
    )

    spark.stop()


if __name__ == "__main__":
    main()
