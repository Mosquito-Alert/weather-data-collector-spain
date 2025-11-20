#!/usr/bin/env python3
"""Summarise Barcelona-specific datasets (historical, hourly, current daily, forecast).

Outputs per-file metadata including date ranges and daily station coverage to stdout.
"""

from __future__ import annotations

import argparse
import csv
import gzip
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple


def open_text(path: Path):
    """Open a text file, falling back to plain open if gzip is not applicable."""

    try:
        return gzip.open(path, "rt")
    except OSError:
        return path.open("rt")


def summarise_daily_file(path: Path, date_col: str, station_col: str) -> Dict[str, object]:
    per_day = defaultdict(set)
    min_date: str | None = None
    max_date: str | None = None
    total_rows = 0

    with open_text(path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            total_rows += 1
            date_val = row.get(date_col)
            station_val = row.get(station_col)
            if not date_val:
                continue
            per_day[date_val].add(station_val)
            min_date = date_val if min_date is None or date_val < min_date else min_date
            max_date = date_val if max_date is None or date_val > max_date else max_date

    per_day_counts = {day: len(stations) for day, stations in per_day.items()}
    return {
        "rows": total_rows,
        "min_date": min_date,
        "max_date": max_date,
        "per_day_counts": per_day_counts,
    }


def summarise_hourly_file(path: Path) -> Dict[str, object]:
    per_day = defaultdict(set)
    min_ts: str | None = None
    max_ts: str | None = None
    total_rows = 0

    with open_text(path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            total_rows += 1
            fint = row.get("fint")
            station = row.get("idema")
            if not fint:
                continue
            day = fint[:10]
            per_day[day].add(station)
            min_ts = fint if min_ts is None or fint < min_ts else min_ts
            max_ts = fint if max_ts is None or fint > max_ts else max_ts

    per_day_counts = {day: len(stations) for day, stations in per_day.items()}
    return {
        "rows": total_rows,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "per_day_counts": per_day_counts,
    }


def summarise_forecast(path: Path) -> Dict[str, object]:
    per_day = defaultdict(int)
    min_date: str | None = None
    max_date: str | None = None
    collected_at: str | None = None
    total_rows = 0

    with open_text(path) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            total_rows += 1
            date_val = row.get("fecha")
            if date_val:
                per_day[date_val] += 1
                min_date = date_val if min_date is None or date_val < min_date else min_date
                max_date = date_val if max_date is None or date_val > max_date else max_date
            ca = row.get("collected_at")
            if ca and (collected_at is None or ca > collected_at):
                collected_at = ca

    return {
        "rows": total_rows,
        "min_date": min_date,
        "max_date": max_date,
        "latest_collected_at": collected_at,
        "per_day_rows": dict(per_day),
    }


def emit_summary(title: str, stats: Dict[str, object], *, last_n: int = 7) -> None:
    print(f"=== {title} ===")
    for key in ("rows", "min_date", "max_date", "min_ts", "max_ts", "latest_collected_at"):
        if key in stats and stats[key] is not None:
            print(f"{key}: {stats[key]}")

    per_day_key = "per_day_counts" if "per_day_counts" in stats else "per_day_rows"
    if per_day_key in stats:
        per_day = stats[per_day_key]
        if per_day:
            recent = sorted(per_day.items())[-last_n:]
            label = "stations_per_day" if per_day_key == "per_day_counts" else "rows_per_day"
            print(f"{label} (last {len(recent)} days):")
            for day, value in recent:
                print(f"  {day}: {value}")
    print()


def main(paths: Iterable[Tuple[str, Path]]) -> None:
    for label, path in paths:
        if not path.exists():
            print(f"=== {label} ===")
            print(f"File not found: {path}")
            print()
            continue

        if label.startswith("Hourly"):
            stats = summarise_hourly_file(path)
        elif label.startswith("Forecast"):
            stats = summarise_forecast(path)
        else:
            stats = summarise_daily_file(path, date_col="fecha", station_col="indicativo")
        emit_summary(label, stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarise Barcelona dataset coverage.")
    parser.add_argument(
        "--daily-historical",
        default="data/output/daily_station_historical_barcelona.csv.gz",
        type=Path,
    )
    parser.add_argument(
        "--hourly",
        default="data/output/hourly_station_ongoing_barcelona.csv.gz",
        type=Path,
    )
    parser.add_argument(
        "--daily-current",
        default="data/output/daily_station_current_barcelona.csv.gz",
        type=Path,
    )
    parser.add_argument(
        "--forecast",
        default="data/output/daily_municipal_forecast_barcelona.csv.gz",
        type=Path,
    )

    args = parser.parse_args()
    targets = [
        ("Historical Daily", args.daily_historical),
        ("Hourly", args.hourly),
        ("Current Daily", args.daily_current),
        ("Forecast", args.forecast),
    ]
    main(targets)