---
title: Weather Data Collector (Spain)
layout: toc
---

# Weather Data Collector (Spain)

Internal documentation for the Mosquito-Alert weather ingestion pipelines. These notes target maintainers running the workloads on the CEAB cluster.

## Architecture Overview

Two independent SLURM pipelines share the same codebase and AEMET API keys:

| Pipeline | Entrypoint | Goal | Key outputs |
|----------|------------|------|-------------|
| **Spain-wide** | `update_weather.sh` (submits `update_municipal_forecasts_only.sh`) | Refresh national historical, current, hourly and municipal forecast datasets | `data/output/daily_station_historical.csv.gz`, `daily_station_current.csv.gz`, `hourly_station_ongoing.csv.gz`, `daily_municipal_forecast.csv.gz` |
| **Barcelona fast-path** | `update_weather_bcn.sh` | Guarantee fresh Barcelona observations and forecasts early each morning | `data/output/daily_station_historical_barcelona.csv.gz`, `daily_station_current_barcelona.csv.gz`, `hourly_station_ongoing_barcelona.csv.gz`, `daily_municipal_forecast_barcelona.csv.gz`, `data/output/barcelona_last_success.txt` |

Both pipelines assume the CEAB modules `GDAL/3.10.0-foss-2024a` and `R/4.4.2-gfbf-2024a` plus a configured key pool in `auth/keys.R`.

## Spain-wide Pipeline

- Launch with `sbatch update_weather.sh` from the project root.
- Sequential steps: `scripts/r/get_historical_data.R`, `aggregate_daily_stations_current.R`, `get_latest_data.R` and SLURM submission of `update_municipal_forecasts_only.sh` (job array for shards).
- Allow ~6 h wall-clock; the municipal array dominates runtime.
- Logs: `logs/update_weather_<jobid>.out|err` for the parent job and `logs/update_municipal_forecasts_<jobid>_<array>.out|err` per shard.
- Outputs land in `data/output/` (nationwide coverage, including Barcelona rows).
- Recommended schedule: overnight once Barcelona fast-path has finished.

## Barcelona Fast-path Pipeline

- Launch with `sbatch update_weather_bcn.sh` (cron currently fires 08:00 CET daily).
- Steps: historical refresh, hourly pull, daily aggregation, municipal forecast (`scripts/r/get_historical_data_barcelona.R`, `get_latest_data_barcelona.R`, `aggregate_daily_stations_current_barcelona.R`, `get_forecast_barcelona.R`).
- Typical runtime ~5 min (historical backfill can extend to ~50 min if the API finally releases new rows).
- Outputs live alongside the nationwide files but carry the `_barcelona` suffix; the hourly script also writes a sentinel timestamp to `data/output/barcelona_last_success.txt`.
- Logs: `logs/update_weather_bcn_<jobid>.out|err`.
- Quick health check: `python3 scripts/python/summarize_barcelona_datasets.py`.

## Data Outputs Summary

| File | Description | Produced by |
|------|-------------|-------------|
| `daily_station_historical.csv.gz` | National historical daily stations (AEMET climatological API, up to ~T-4 days). | Spain-wide |
| `daily_station_current.csv.gz` | Daily aggregates derived from the recent hourly feed (bridges the historical lag). | Spain-wide |
| `hourly_station_ongoing.csv.gz` | Nationwide hourly observations in long format (`fint`, `idema`, `measure`, `value`). | Spain-wide |
| `daily_municipal_forecast.csv.gz` | Municipal forecasts for all CUMUN codes; each elaborado snapshot retained. | Spain-wide |
| `daily_station_*_barcelona.csv.gz` | Barcelona-only mirrors of the station datasets. | Barcelona fast-path |
| `hourly_station_ongoing_barcelona.csv.gz` | Barcelona hourly feed (same schema as nationwide file). | Barcelona fast-path |
| `daily_municipal_forecast_barcelona.csv.gz` | Barcelona municipal forecast, multiple elaborados stored. | Barcelona fast-path |
| `barcelona_last_success.txt` | UTC timestamp of the last successful Barcelona hourly run. | Barcelona fast-path |

Legacy municipal forecast dumps (`municipal_forecasts_*.csv[.gz]` and intermediates) remain for audit but are not regenerated.

## Operations

### Modules and Environment

```bash
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a
```

`auth/keys.R` exposes helper functions (`set_active_key_pool`, `get_current_api_key`, `rotate_api_key`) that both pipelines rely on. Keep that file off git.

### Running Jobs Manually

```bash
cd /home/j.palmer/research/weather-data-collector-spain

# Barcelona fast-path
sbatch update_weather_bcn.sh

# Optional Barcelona historical backfill when climatological API releases new days
sbatch scripts/bash/get_historical_barcelona.sh

# Spain-wide refresh
sbatch update_weather.sh
```

To debug interactively, load the modules above on a compute node and call the relevant `Rscript` directly.

### Monitoring

- **Logs**: all SLURM output resides in `logs/`. Review both `.out` and `.err` files—collectors log warnings inline, so empty `.err` files usually indicate success.
- **Barcelona snapshot**: `python3 scripts/python/summarize_barcelona_datasets.py` prints row counts and date ranges for the four Barcelona datasets.
- **Nationwide municipal coverage**: evaluate `logs/update_municipal_forecasts_<jobid>_<array>.out` to see remaining municipality IDs per shard; persistent failures are tracked in `docs/pending_municipal_id_fix.md`.
- **Sentinel**: `cat data/output/barcelona_last_success.txt` should show the latest hourly collection timestamp.

## Known Behaviours

- **Climatological lag**: `get_historical_data*_barcelona.R` may print `No hay datos que satisfagan esos criterios`—AEMET normally publishes daily records 4–7 days late. The script logs the gap and exits cleanly.
- **Hourly 404 vs 429**: 404 means the station has nothing new; we skip without key rotation. 429 triggers rotation with exponential backoff.
- **Forecast elaborados**: Multiple elaborados per day are persisted so we can reconstruct forecast evolution. Downstream consumers that only need the freshest view should filter to the maximum `elaborado` per `fecha`.
- **Municipality IDs**: Zero-padding of nationwide `municipio_id` values is being normalised (see `docs/pending_municipal_id_fix.md`). Expect legacy rows without padding until the remediation backfill runs.

## File Layout

```
auth/                      # AEMET credential helpers (ignored by git)
code/                      # Legacy scripts (kept for reference)
data/
    ├─ input/                # Station + municipality reference tables
    └─ output/               # Generated datasets (nationwide + Barcelona)
docs/                      # Internal documentation (this site)
logs/                      # SLURM stdout/err
scripts/
    ├─ r/                    # Active collectors and utilities
    ├─ bash/                 # SLURM wrappers
    └─ archive/              # Retired entrypoints
```

## Maintenance Checklist

- Ensure both pipelines stay green in SLURM after any code change or key rotation.
- Keep the Barcelona cron entry at 08:00 CET (plus any earlier manual run when dashboards require it).
- Monitor API quotas; rotate or add keys in `auth/keys.R` as needed.
- Periodically purge old `municipal_forecasts_*` dumps if disk pressure rises.
- Update this documentation and the root README when operational procedures change.

For outstanding remediation items (e.g., municipality ID padding) consult the notes in `docs/`.
