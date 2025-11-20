# Weather Data Collector (Spain)

Operational runbook for the Mosquito-Alert weather ingestion pipelines on the CEAB cluster. The system harvests AEMET OpenData products nationwide and keeps a separate fast-path for Barcelona so dashboards receive fresh forecasts early each morning.

## Pipelines at a Glance

| Pipeline | Purpose | SLURM entrypoint | Typical runtime | Outputs |
|----------|---------|------------------|-----------------|---------|
| **Spain-wide** | Refresh nationwide historical, current, hourly, and municipal forecast datasets (Barcelona included) | `update_weather.sh` (chains `update_municipal_forecasts_only.sh`) | ~3–4 h wall-clock (allow 6 h allocation) | `data/output/daily_station_historical.csv.gz`, `data/output/daily_station_current.csv.gz`, `data/output/hourly_station_ongoing.csv.gz`, `data/output/daily_municipal_forecast.csv.gz` |
| **Barcelona fast-path** | Ensure Barcelona observations and forecasts are up-to-date before business hours | `update_weather_bcn.sh` | ~5 min steady-state (≤50 min when historical API finally releases new rows) | `data/output/daily_station_historical_barcelona.csv.gz`, `data/output/daily_station_current_barcelona.csv.gz`, `data/output/hourly_station_ongoing_barcelona.csv.gz`, `data/output/daily_municipal_forecast_barcelona.csv.gz`, sentinel `data/output/barcelona_last_success.txt` |

Both pipelines require the modules `GDAL/3.10.0-foss-2024a` and `R/4.4.2-gfbf-2024a`, plus active credentials managed by helpers in `auth/keys.R`.

## Requirements

- CEAB SLURM access with ability to load the modules above.
- AEMET OpenData key pool configured via `auth/keys.R` (not tracked in git).
- Writable `data/output/` and `logs/` directories on the project filesystem.

## Running Jobs

SLURM wrappers load the required GDAL and R modules internally; load them manually only when running the underlying `Rscript` files yourself.

```bash
cd /home/j.palmer/research/weather-data-collector-spain

# Barcelona fast-path (cron: 08:00 CET)
sbatch update_weather_bcn.sh

# Optional Barcelona historical backfill when API publishes new rows
sbatch scripts/bash/get_historical_barcelona.sh

# Spain-wide refresh (run after Barcelona pipeline completes)
sbatch update_weather.sh
```

Submit from the project root so relative paths resolve. For debugging, launch an interactive SLURM session, load the modules, and run the relevant `Rscript` entrypoints directly.

## Monitoring

- **Logs**: Inspect `logs/update_weather_<jobid>.out|err` for the Spain-wide parent job and `logs/update_municipal_forecasts_<jobid>_<array>.out|err` for municipal shards. Barcelona runs log to `logs/update_weather_bcn_<jobid>.out|err`.
- **Barcelona health check**: `python3 scripts/python/summarize_barcelona_datasets.py` prints row counts and date ranges for the four Barcelona datasets.
- **Sentinel**: `cat data/output/barcelona_last_success.txt` shows the timestamp of the last successful Barcelona hourly pull; monitoring tooling relies on this value.
- **Municipality coverage**: Review shard logs for persistent `municipio_id` failures. Open remediation items are tracked in `docs/pending_municipal_id_fix.md`.

## Data Outputs

| File | Description | Pipeline |
|------|-------------|----------|
| `data/output/daily_station_historical.csv.gz` | Historical daily observations (AEMET climatological endpoint, ~4–7 day publication lag). | Spain-wide |
| `data/output/daily_station_current.csv.gz` | Daily aggregates built from recent hourly feeds, bridging the climatological lag. | Spain-wide |
| `data/output/hourly_station_ongoing.csv.gz` | Nationwide hourly long-format feed (`fint`, `idema`, `measure`, `value`). | Spain-wide |
| `data/output/daily_municipal_forecast.csv.gz` | All municipalities, retaining each elaborado snapshot for forecast evolution analysis. | Spain-wide |
| `data/output/daily_station_historical_barcelona.csv.gz` | Barcelona subset of the historical daily observations. | Barcelona fast-path |
| `data/output/daily_station_current_barcelona.csv.gz` | Barcelona subset of the daily aggregates derived from hourly data. | Barcelona fast-path |
| `data/output/hourly_station_ongoing_barcelona.csv.gz` | Barcelona hourly feed with identical schema to the nationwide file. | Barcelona fast-path |
| `data/output/daily_municipal_forecast_barcelona.csv.gz` | Barcelona municipal forecast, keeping multiple elaborados per day. | Barcelona fast-path |
| `data/output/barcelona_last_success.txt` | UTC timestamp of the last successful Barcelona hourly job. | Barcelona fast-path |

Legacy municipal dumps (`data/output/municipal_forecasts_*.csv[.gz]`) remain for auditing but are no longer regenerated.

## Known Behaviours and Troubleshooting

- **Historical lag**: `scripts/r/get_historical_data*.R` may log `No hay datos que satisfagan esos criterios` until AEMET publishes the day. The job still exits successfully.
- **Hourly 404 vs 429**: 404 simply means no new records for that station; 429 triggers API key rotation and retry backoff.
- **Forecast elaborados**: Multiple elaborados per day are expected. Downstream consumers needing the freshest forecast should filter to the maximum `elaborado` per `fecha`.
- **Municipio padding**: Some legacy rows lack zero-padding. Remediation steps and outstanding IDs live in `docs/pending_municipal_id_fix.md`.

## Repository Layout

```
auth/                      # Credential helpers (ignored by git)
code/                      # Legacy R scripts (reference only)
data/
    ├─ input/                # Static reference tables
    └─ output/               # Generated datasets (nationwide + Barcelona)
docs/                      # Internal documentation (Jekyll site)
logs/                      # SLURM stdout/err archives
scripts/
    ├─ r/                    # Active collectors and utilities
    ├─ bash/                 # SLURM wrappers and helpers
    └─ archive/              # Retired entrypoints
update_weather.sh          # Spain-wide pipeline launcher
update_weather_bcn.sh      # Barcelona fast-path launcher
```

## Maintenance Checklist

- Keep both pipelines green after code or configuration changes; verify SLURM logs before closing incidents.
- Ensure the Barcelona cron entry remains at 08:00 CET (adjust only with stakeholder agreement).
- Rotate or expand the key pool in `auth/keys.R` when rate-limit warnings appear.
- Reclaim disk space periodically by pruning obsolete `municipal_forecasts_*` archives once validated elsewhere.
- Update this README and `docs/index.md` whenever operational steps or outputs change.

For open remediation items (e.g., municipality ID normalisation) consult the working notes under `docs/`.
├── auth/                  # Untracked directory for API keys
│   └── keys.R
├── code/                  # Main R scripts
│   ├── get_historical_data.R      # Historical daily weather
│   ├── get_latest_data.R          # Basic current observations (5 vars)
│   ├── get_latest_data_expanded.R # Enhanced observations (7 safe vars)
│   ├── get_forecast_data.R        # Municipal forecasts (enhanced)
│   ├── get_forecast_data_simple.R # Municipal forecasts (robust)
│   ├── variable_compatibility_analysis.R # Variable analysis
│   ├── aggregate_daily_station_data.R    # Daily aggregations
│   └── aggregate_municipal_data.R        # Municipal processing
├── data/                  # Output data files
│   ├── spain_weather.csv.gz             # Basic observations (5 vars)
│   ├── spain_weather_expanded.csv.gz    # Enhanced observations (7 vars)
│   ├── spain_weather_daily_historical.csv.gz # Historical daily data
│   ├── municipalities.csv.gz            # All Spanish municipalities (8,129)
│   └── AEMET_variable_documentation.md  # Variable reference
├── docs/                  # Documentation and analysis
│   ├── index.md                         # GitHub Pages site
│   ├── variables.md                     # Variable documentation
│   ├── api-analysis.md                  # API endpoint analysis
│   └── forecast-collection.md           # Forecast system guide
├── logs/                  # Log files and script outputs
├── renv/                  # R environment and package management
├── update_weather.sh      # Shell script for automation
├── update_historical_weather.sh
├── README.md
└── ...
```

## Notes
- The `auth/` directory is not tracked by git for security.
- Scripts are designed to be robust to API failures and rate limits.
- For more details, see comments in each script.

## License
See `LICENSE` file for details.
