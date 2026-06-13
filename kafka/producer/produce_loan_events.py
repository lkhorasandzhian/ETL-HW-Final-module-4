#!/usr/bin/env python3
"""
Send generated JSONL events to a Kafka topic.

Usage:
  python kafka/producer/produce_loan_events.py \
    --bootstrap-servers <host>:9091 \
    --topic loan-events \
    --input data/generated/loan_events.jsonl
"""
from __future__ import annotations

import argparse
from pathlib import Path
from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--input", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=50,
        batch_size=32_768,
    )

    sent = 0
    with args.input.open("r", encoding="utf-8") as f:
        for line in f:
            producer.send(args.topic, line.rstrip("\n"))
            sent += 1
            if sent % 10_000 == 0:
                print(f"Sent {sent:,} events")

    producer.flush()
    producer.close()
    print(f"Done. Sent {sent:,} events to topic {args.topic}")


if __name__ == "__main__":
    main()
