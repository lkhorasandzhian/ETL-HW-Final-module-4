#!/usr/bin/env python3
"""
PySpark batch job for Task 2.
Reads applications CSV from Object Storage/HDFS-compatible path, aggregates metrics,
and writes results back to Object Storage as Parquet.

Example local run:
  spark-submit spark/batch/process_applications.py \
    --input data/generated/applications.csv \
    --output data/output/applications_agg

Example cloud run paths:
  --input s3a://<bucket>/raw/applications/applications.csv
  --output s3a://<bucket>/processed/applications_agg
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("etl-module4-applications-batch")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args.input)
    )

    result = (
        df.withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date", "region_code", "product_type", "risk_level", "decision_status")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("requested_amount").alias("requested_amount_total"),
            F.sum("approved_amount").alias("approved_amount_total"),
            F.avg("credit_score").alias("avg_credit_score"),
            F.avg("processing_time_sec").alias("avg_processing_time_sec"),
        )
    )

    result.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
