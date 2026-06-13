#!/usr/bin/env python3
"""
Generate synthetic loan_events.jsonl for the Kafka + PySpark task.
Default target size: 25 MiB, satisfying the 20+ MB requirement.

Each line is a JSON object suitable for sending to a Kafka topic.

Usage:
  python scripts/generate_loan_events_jsonl.py --output data/generated/loan_events.jsonl --target-mb 25
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "FR-IDF", "NL-NH", "ES-MD", "IT-LAZ"]
RISK_LEVELS = ["low", "medium", "high"]
DOC_TYPES = ["passport", "income_statement", "bank_statement", "employment_contract"]
DOC_STATUSES = ["verified", "pending", "rejected"]


def build_event(i: int, rng: random.Random, start: datetime) -> dict:
    score = int(max(300, min(850, rng.gauss(670, 90))))
    risk = "low" if score >= 720 else "medium" if score >= 620 else "high"
    decision = rng.choices(
        ["approved", "manual_review", "rejected"],
        weights=[70, 22, 8] if risk == "low" else [42, 38, 20] if risk == "medium" else [10, 35, 55],
        k=1,
    )[0]
    submitted_at = start + timedelta(seconds=rng.randint(0, 60 * 60 * 24 * 90))

    docs_count = rng.randint(1, 4)
    docs = [
        {"type": doc_type, "status": rng.choice(DOC_STATUSES)}
        for doc_type in rng.sample(DOC_TYPES, k=docs_count)
    ]

    return {
        "application_id": f"loan_{i:09d}",
        "customer": {
            "customer_id": f"cust_{rng.randint(10000, 9999999)}",
            "region": rng.choice(REGIONS),
        },
        "loan": {
            "amount": rng.randrange(1_000, 80_001, 500),
            "term_months": rng.choice([6, 12, 18, 24, 36, 48, 60]),
        },
        "scoring": {
            "score": score,
            "risk_level": risk,
        },
        "documents": docs,
        "decision_status": decision,
        "submitted_at": submitted_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate(output: Path, target_mb: int, seed: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = target_mb * 1024 * 1024
    rng = random.Random(seed)
    start = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)

    with output.open("wb") as f:
        i = 1
        while f.tell() < target_bytes:
            line = json.dumps(build_event(i, rng, start), ensure_ascii=False, separators=(",", ":")) + "\n"
            f.write(line.encode("utf-8"))
            i += 1

    size_mb = output.stat().st_size / 1024 / 1024
    print(f"Generated {output} | events={i - 1:,} | size={size_mb:.2f} MiB")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/generated/loan_events.jsonl"))
    parser.add_argument("--target-mb", type=int, default=25)
    parser.add_argument("--seed", type=int, default=44)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate(args.output, args.target_mb, args.seed)
