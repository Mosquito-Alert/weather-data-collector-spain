# Weather Data Collection Workflow

## Overview
The comprehensive aggregation approach systematically produces all 3 required datasets as specified in specs.md.

## Main Workflow Scripts

### Data Collection Scripts
1. **`get_forecast_data.R`** - Collects municipal 7-day forecasts
2. **`get_latest_data.R`** - Collects hourly observations (Dataset 3)  
3. **`get_historical_data.R`** - Collects historical daily data (optional)

### Aggregation Scripts (Comprehensive Approach)
4. **`aggregate_daily_station_data.R`** - Creates Dataset 1 (Daily station data)
5. **`aggregate_municipal_daily.R`** - Creates Dataset 2 (Municipal daily + forecasts)

### Orchestration Scripts
- **`generate_all_datasets.sh`** - Runs all aggregation scripts in correct order
- **`update_weather.sh`** - Complete workflow: collection → aggregation

## Output Datasets

| Dataset | File | Description | Source |
|---------|------|-------------|--------|
| **Dataset 1** | `data/output/daily_station_aggregated.csv.gz` | Daily weather by station (historical + current) | Historical API + hourly aggregation |
| **Dataset 2** | `data/output/daily_municipal_extended.csv.gz` | Municipal daily data + 7-day forecasts | Station aggregation + municipal forecasts |
| **Dataset 3** | `data/output/hourly_station_ongoing.csv.gz` | Hourly station observations archive | Current observations API |

## Execution

### Full Workflow
```bash
./update_weather.sh
```

### Datasets Only (if data already collected)
```bash
./generate_all_datasets.sh
```

### Individual Collection Scripts
```bash
Rscript code/get_forecast_data.R
Rscript code/get_latest_data.R
Rscript code/get_historical_data.R
```

## Deprecated Scripts

- **`generate_municipal_priority.R`** - No longer used in main workflow
  - Remains available for emergency/priority use cases
  - Was previously used for immediate municipal data needs

## File Structure

```
data/
├── input/
│   └── municipalities.csv.gz           # Municipality reference data
└── output/
    ├── daily_station_aggregated.csv.gz       # Dataset 1
    ├── daily_municipal_extended.csv.gz       # Dataset 2  
    ├── hourly_station_ongoing.csv.gz         # Dataset 3
    ├── municipal_forecasts_YYYY-MM-DD.csv    # Raw forecasts
    └── daily_station_historical.csv.gz       # Historical data (optional)
```

## Monitoring

- Log files are saved to `logs/` with timestamps
- Status reporting via `scripts/update_weather_status.sh`
- Error checking and validation built into aggregation scripts

## Dependencies

All scripts use the standardized 7-variable approach:
- `ta` - Air temperature (°C)
- `tamax` - Maximum temperature (°C)  
- `tamin` - Minimum temperature (°C)
- `hr` - Relative humidity (%)
- `prec` - Precipitation (mm)
- `vv` - Wind speed (km/h)
- `pres` - Atmospheric pressure (hPa)
