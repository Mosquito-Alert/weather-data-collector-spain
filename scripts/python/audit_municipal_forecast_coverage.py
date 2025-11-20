#!/usr/bin/env python3
"""Audit municipal forecast coverage against the reference list."""

from __future__ import annotations

import csv
import gzip
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REF_PATH = PROJECT_ROOT / "data/input/municipalities.csv.gz"
FORECAST_PATH = PROJECT_ROOT / "data/output/daily_municipal_forecast.csv.gz"

NEW_MUNICIPIOS = {
    "11903",  # San Martín del Tesorillo
    "14901",  # Fuente Carreteros
    "14902",  # La Guijarrosa
    "18077",  # Fornes
    "21902",  # La Zarza-Perrunal
    "41904",  # El Palmar de Troya
}

COMMUNAL_CODES = {
    "53000", "53001", "53002", "53003", "53004", "53005", "53006", "53007",
    "53008", "53009", "53010", "53011", "53012", "53013", "53014", "53015",
    "53016", "53017", "53018", "53019", "53020", "53021", "53022", "53023",
    "53024", "53025", "53026", "53027", "53028", "53029", "53031", "53032",
    "53033", "53034", "53035", "53036", "53037", "53038", "53039", "53040",
    "53041", "53042", "53043", "53044", "53045", "53046", "53047", "53048",
    "53049", "53050", "53051", "53052", "53053", "53054", "53055", "53056",
    "53057", "53058", "53059", "53060", "53061", "53062", "53063", "53064",
    "53065", "53066", "53067", "53068", "53069", "53070", "53071", "53072",
    "53073", "53074", "53075", "53076", "53077", "53078", "53080", "53081",
    "53083", "54001", "54002", "54003", "54004", "54005",
}

EXPECTED_ABSENT = NEW_MUNICIPIOS | COMMUNAL_CODES


def load_reference_ids(path: Path) -> set[str]:
    if not path.exists():
        print(f"ERROR: reference file not found: {path}", file=sys.stderr)
        sys.exit(2)
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "CUMUN" not in reader.fieldnames:
            print("ERROR: reference file missing CUMUN header", file=sys.stderr)
            sys.exit(2)
        ids = set()
        for row in reader:
            raw = row.get("CUMUN")
            if raw is None:
                continue
            stripped = raw.strip()
            if not stripped:
                continue
            ids.add(stripped.zfill(5))
    return ids


def load_forecast_ids(path: Path) -> set[str]:
    if not path.exists():
        print(f"ERROR: forecast file not found: {path}", file=sys.stderr)
        sys.exit(2)
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "municipio_id" not in reader.fieldnames:
            print("ERROR: forecast file missing municipio_id header", file=sys.stderr)
            sys.exit(2)
        ids = set()
        for row in reader:
            mid = row.get("municipio_id")
            if mid:
                ids.add(mid)
    return ids


def main() -> int:
    ref_ids = load_reference_ids(REF_PATH)
    forecast_ids = load_forecast_ids(FORECAST_PATH)

    missing = sorted(ref_ids - forecast_ids - EXPECTED_ABSENT)
    unexpected_present = sorted(forecast_ids & EXPECTED_ABSENT)

    print(f"Reference municipalities: {len(ref_ids)}")
    print(f"Forecast municipalities: {len(forecast_ids)}")
    print(f"Ignored IDs (expected absent): {len(EXPECTED_ABSENT)}")

    if unexpected_present:
        print(
            "WARNING: expected-absent IDs present in forecast data: "
            + ", ".join(unexpected_present)
        )

    if missing:
        print(f"ERROR: {len(missing)} reference municipios missing from forecasts.")
        print(
            "Sample missing IDs: " + ", ".join(missing[:20])
            + ("..." if len(missing) > 20 else "")
        )
        return 1

    print("Municipal forecast coverage OK (excluding expected gaps).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
