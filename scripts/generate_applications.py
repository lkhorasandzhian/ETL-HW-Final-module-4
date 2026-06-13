#!/usr/bin/env python3
"""
Generate synthetic applications.csv for the Airflow + Yandex Data Processing task.
Default target size: 60 MiB, satisfying the 50+ MB requirement.

Usage:
  python scripts/generate_applications.py --output data/generated/applications.csv --target-mb 60
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

HEADER = (
    "application_id,event_time,customer_id,region_code,product_type,requested_amount,"
    "term_months,credit_score,risk_level,decision_status,approved_amount,channel,"
    "employee_review_flag,processing_time_sec\n"
)
REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "FR-IDF", "NL-NH", "ES-MD", "IT-LAZ"]
PRODUCTS = ["cash_loan", "credit_card", "mortgage", "car_loan", "bnpl"]
CHANNELS = ["mobile", "web", "branch", "call_center", "partner_api"]


def risk_from_score(score: int) -> str:
    if score >= 720:
        return "low"
    if score >= 620:
        return "medium"
    return "high"


def decision_from_risk(risk: str, rng: random.Random) -> str:
    if risk == "low":
        return rng.choices(["approved", "manual_review", "rejected"], weights=[84, 13, 3], k=1)[0]
    if risk == "medium":
        return rng.choices(["approved", "manual_review", "rejected"], weights=[48, 35, 17], k=1)[0]
    return rng.choices(["approved", "manual_review", "rejected"], weights=[12, 38, 50], k=1)[0]


def build_row(i: int, rng: random.Random, start: datetime) -> str:
    event_time = start + timedelta(seconds=rng.randint(0, 60 * 60 * 24 * 90))
    score = int(max(300, min(850, rng.gauss(670, 85))))
    risk = risk_from_score(score)
    decision = decision_from_risk(risk, rng)
    requested = rng.randrange(1_000, 80_001, 500)
    approved = 0 if decision == "rejected" else int(requested * rng.choice([1.0, 0.9, 0.8, 0.7]))
    review = "true" if decision == "manual_review" or risk == "high" else rng.choice(["false", "false", "true"])
    processing_time = rng.randint(3, 25) if review == "false" else rng.randint(60, 1800)

    return (
        f"app_20260501_{i:09d},"
        f"{event_time:%Y-%m-%d %H:%M:%S},"
        f"cust_{rng.randint(10000, 9999999)},"
        f"{rng.choice(REGIONS)},"
        f"{rng.choice(PRODUCTS)},"
        f"{requested},"
        f"{rng.choice([6, 12, 18, 24, 36, 48, 60])},"
        f"{score},"
        f"{risk},"
        f"{decision},"
        f"{approved},"
        f"{rng.choice(CHANNELS)},"
        f"{review},"
        f"{processing_time}\n"
    )


def generate(output: Path, target_mb: int, seed: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = target_mb * 1024 * 1024
    rng = random.Random(seed)
    start = datetime(2026, 5, 1, 0, 0, 0)

    with output.open("wb") as f:
        f.write(HEADER.encode("utf-8"))
        i = 1
        while f.tell() < target_bytes:
            f.write(build_row(i, rng, start).encode("utf-8"))
            i += 1

    size_mb = output.stat().st_size / 1024 / 1024
    print(f"Generated {output} | rows={i - 1:,} | size={size_mb:.2f} MiB")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/generated/applications.csv"))
    parser.add_argument("--target-mb", type=int, default=60)
    parser.add_argument("--seed", type=int, default=43)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate(args.output, args.target_mb, args.seed)
