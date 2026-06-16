#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--input", type=Path, required=True)

    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--ssl-cafile", type=Path, required=True)

    parser.add_argument("--batch-confirm-size", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    if not args.ssl_cafile.exists():
        raise FileNotFoundError(f"SSL CA file not found: {args.ssl_cafile}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=args.username,
        sasl_plain_password=args.password,
        ssl_cafile=str(args.ssl_cafile),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=50,
        batch_size=32_768,
        retries=5,
    )

    sent = 0
    sent_bytes = 0
    pending = []

    with args.input.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            value = line.rstrip("\n")
            if not value:
                continue

            try:
                event = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_no}: {exc}") from exc

            key = event.get("application_id")

            future = producer.send(args.topic, key=key, value=value)
            pending.append(future)

            sent += 1
            sent_bytes += len(value.encode("utf-8"))

            if len(pending) >= args.batch_confirm_size:
                for item in pending:
                    item.get(timeout=30)
                pending.clear()

            if sent % 10_000 == 0:
                mb = sent_bytes / 1024 / 1024
                print(f"Sent {sent:,} events, {mb:.2f} MiB")

    for item in pending:
        item.get(timeout=30)

    producer.flush()
    producer.close()

    mb = sent_bytes / 1024 / 1024
    print(f"Done. Sent {sent:,} events to topic {args.topic}")
    print(f"Total payload size: {mb:.2f} MiB")


if __name__ == "__main__":
    main()
