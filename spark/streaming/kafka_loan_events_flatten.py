#!/usr/bin/env python3
"""
PySpark Structured Streaming job for Task 3.
Reads JSON events from Kafka, flattens nested fields, and writes them to Object Storage.

Example:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    spark/streaming/kafka_loan_events_flatten.py \
    --bootstrap-servers <kafka-bootstrap>:9091 \
    --topic loan-events \
    --checkpoint s3a://<bucket>/checkpoints/loan_events_flatten \
    --output s3a://<bucket>/processed/loan_events_flat
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


SCHEMA = T.StructType([
    T.StructField("application_id", T.StringType()),
    T.StructField("customer", T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("region", T.StringType()),
    ])),
    T.StructField("loan", T.StructType([
        T.StructField("amount", T.IntegerType()),
        T.StructField("term_months", T.IntegerType()),
    ])),
    T.StructField("scoring", T.StructType([
        T.StructField("score", T.IntegerType()),
        T.StructField("risk_level", T.StringType()),
    ])),
    T.StructField("documents", T.ArrayType(T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("status", T.StringType()),
    ]))),
    T.StructField("decision_status", T.StringType()),
    T.StructField("submitted_at", T.StringType()),
])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("etl-module4-kafka-loan-events-flatten").getOrCreate()

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("event"))

    flat = parsed.select(
        F.col("event.application_id").alias("application_id"),
        F.col("event.customer.customer_id").alias("customer_id"),
        F.col("event.customer.region").alias("region_code"),
        F.col("event.loan.amount").alias("amount"),
        F.col("event.loan.term_months").alias("term_months"),
        F.col("event.scoring.score").alias("score"),
        F.col("event.scoring.risk_level").alias("risk_level"),
        F.col("event.decision_status").alias("decision_status"),
        F.to_timestamp("event.submitted_at").alias("submitted_at"),
        F.size("event.documents").alias("documents_count"),
        F.expr("exists(event.documents, x -> x.status = 'rejected')").alias("has_rejected_document"),
    )

    query = (
        flat.writeStream
        .format("parquet")
        .option("checkpointLocation", args.checkpoint)
        .option("path", args.output)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
