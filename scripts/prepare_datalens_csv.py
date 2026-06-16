from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]

BATCH_SRC = ROOT_DIR / "data" / "output" / "datalens" / "batch_parts"
STREAMING_SRC = ROOT_DIR / "data" / "output" / "datalens" / "streaming_parts"

OUT_DIR = ROOT_DIR / "data" / "output" / "datalens"
BATCH_OUT = OUT_DIR / "batch_applications_datalens.csv"
STREAMING_OUT = OUT_DIR / "streaming_loan_events_datalens.csv"


def parquet_parts_to_csv(src_dir: Path, out_file: Path) -> None:
    part_files = sorted(src_dir.glob("part-*.parquet"))

    if not part_files:
        raise FileNotFoundError(f"No parquet part files found in: {src_dir}")

    frames = []
    for file_path in part_files:
        print(f"Reading: {file_path}")
        frames.append(pd.read_parquet(file_path))

    df = pd.concat(frames, ignore_index=True)

    print(f"Writing: {out_file}")
    df.to_csv(out_file, index=False, encoding="utf-8")

    print(
        f"Done: {out_file.name} | "
        f"rows={len(df)} | columns={len(df.columns)} | "
        f"size_mb={out_file.stat().st_size / 1024 / 1024:.2f}"
    )
    print("Columns:")
    for column in df.columns:
        print(f"  - {column}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    parquet_parts_to_csv(BATCH_SRC, BATCH_OUT)
    parquet_parts_to_csv(STREAMING_SRC, STREAMING_OUT)


if __name__ == "__main__":
    main()
