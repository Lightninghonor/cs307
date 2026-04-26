from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path


def parse_date(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y/%m/%d")


def format_date(value: datetime) -> str:
    return value.strftime("%Y/%m/%d")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a larger tickets CSV for Task 3.3 experiments")
    parser.add_argument("--input", default="tickets.csv", help="Source tickets CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument("--multiplier", type=int, default=5, help="How many copies to generate")
    parser.add_argument(
        "--date-step-days",
        type=int,
        default=30,
        help="Shift each copy by this many days to keep (route_id, flight_date) unique",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.multiplier < 1:
        raise ValueError("--multiplier must be >= 1")
    if args.date_step_days < 0:
        raise ValueError("--date-step-days must be >= 0")

    input_path = Path(args.input)
    output_path = Path(args.output)

    with input_path.open("r", encoding="utf-8-sig", newline="") as source_handle:
        reader = csv.DictReader(source_handle)
        rows = list(reader)
        fieldnames = reader.fieldnames

    if not fieldnames:
        raise RuntimeError("Input CSV has no header")
    if "date" not in fieldnames:
        raise RuntimeError("Input CSV must contain a 'date' column")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as target_handle:
        writer = csv.DictWriter(target_handle, fieldnames=fieldnames)
        writer.writeheader()

        for copy_index in range(args.multiplier):
            delta = timedelta(days=copy_index * args.date_step_days)
            for row in rows:
                new_row = dict(row)
                new_row["date"] = format_date(parse_date(row["date"]) + delta)
                writer.writerow(new_row)

    print(f"Input rows: {len(rows)}")
    print(f"Output rows: {len(rows) * args.multiplier}")
    print(f"Output file: {output_path}")


if __name__ == "__main__":
    main()
