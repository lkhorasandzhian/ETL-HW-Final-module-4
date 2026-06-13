#!/usr/bin/env python3
"""Generate all synthetic datasets for the ETL module 4 exam project."""
from pathlib import Path

from generate_applications import generate as generate_applications
from generate_loan_events_jsonl import generate as generate_loan_events
from generate_transactions_v2 import generate as generate_transactions

BASE_DIR = Path("data/generated")

def main() -> None:
    generate_transactions(BASE_DIR / "transactions_v2.csv", target_mb=35, seed=42)
    generate_applications(BASE_DIR / "applications.csv", target_mb=60, seed=43)
    generate_loan_events(BASE_DIR / "loan_events.jsonl", target_mb=25, seed=44)

if __name__ == "__main__":
    main()
