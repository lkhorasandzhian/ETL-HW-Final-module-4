#!/usr/bin/env python3

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
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)

    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=200000)

    return parser.parse_args()


def escape_jaas_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("etl-module4-kafka-loan-events-flatten")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    username = escape_jaas_value(args.username)
    password = escape_jaas_value(args.password)

    jaas_config = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{username}" '
        f'password="{password}";'
    )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )

    kafka_messages = raw.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
    )

    parsed = kafka_messages.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
        F.from_json(F.col("raw_json"), SCHEMA).alias("event"),
    ).where(F.col("event").isNotNull())

    with_documents = parsed.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.col("event.application_id").alias("application_id"),
        F.col("event.customer.customer_id").alias("customer_id"),
        F.col("event.customer.region").alias("region_code"),
        F.col("event.loan.amount").alias("loan_amount"),
        F.col("event.loan.term_months").alias("loan_term_months"),
        F.col("event.scoring.score").alias("scoring_score"),
        F.col("event.scoring.risk_level").alias("scoring_risk_level"),
        F.col("event.decision_status").alias("decision_status"),
        F.to_timestamp(
            F.col("event.submitted_at"),
            "yyyy-MM-dd'T'HH:mm:ssX",
        ).alias("submitted_at"),
        F.size("event.documents").alias("documents_count"),
        F.explode_outer(F.col("event.documents")).alias("document"),
    )

    flat = with_documents.select(
        "application_id",
        "customer_id",
        "region_code",
        "loan_amount",
        "loan_term_months",
        "scoring_score",
        "scoring_risk_level",
        F.col("document.type").alias("document_type"),
        F.col("document.status").alias("document_status"),
        "documents_count",
        "decision_status",
        "submitted_at",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("processed_at"),
    )

    query = (
        flat.writeStream
        .trigger(once=True)
        .format("parquet")
        .option("checkpointLocation", args.checkpoint)
        .option("path", args.output)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
