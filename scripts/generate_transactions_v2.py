#!/usr/bin/env python3
"""
Generate synthetic transactions_v2.csv for Yandex DataTransfer/YDB task.
Default target size: 35 MiB, satisfying the 30+ MB requirement.

Usage:
  python scripts/generate_transactions_v2.py --output data/generated/transactions_v2.csv --target-mb 35
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

HEADER = "call_id,call_time,client_id,region_code,campaign_type,call_status,client_response,duration_sec,follow_up_required\n"
REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "FR-IDF", "NL-NH", "ES-MD", "IT-LAZ"]
CAMPAIGNS = ["credit_card_offer", "cash_loan", "mortgage_refinance", "deposit_offer", "insurance_bundle"]
CALL_STATUSES = ["answered", "missed", "busy", "failed", "voicemail"]
RESPONSES = ["interested", "not_interested", "callback_requested", "no_answer", "needs_more_info"]


def build_row(i: int, rng: random.Random, start: datetime) -> str:
    call_time = start + timedelta(seconds=rng.randint(0, 60 * 60 * 24 * 60))
    call_status = rng.choice(CALL_STATUSES)
    if call_status != "answered":
        response = "no_answer"
        duration = rng.randint(0, 45)
        follow_up = rng.choice(["false", "true", "false"])
    else:
        response = rng.choice(RESPONSES)
        duration = rng.randint(35, 1200)
        follow_up = "true" if response in {"interested", "callback_requested", "needs_more_info"} else "false"

    return (
        f"call_20260501_{i:09d},"
        f"{call_time:%Y-%m-%d %H:%M:%S},"
        f"client_{rng.randint(1000, 999999)},"
        f"{rng.choice(REGIONS)},"
        f"{rng.choice(CAMPAIGNS)},"
        f"{call_status},"
        f"{response},"
        f"{duration},"
        f"{follow_up}\n"
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
    parser.add_argument("--output", type=Path, default=Path("data/generated/transactions_v2.csv"))
    parser.add_argument("--target-mb", type=int, default=35)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate(args.output, args.target_mb, args.seed)
