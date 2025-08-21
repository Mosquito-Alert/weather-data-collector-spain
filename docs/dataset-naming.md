# Dataset Naming Convention

## Proposed Filenames:

### Dataset 1: Daily Station Data
- **Current**: `daily_station_historical.csv.gz`
- **Coverage**: Historical (2013+) through present day
- **Sources**: AEMET historical endpoint + recent observations

### Dataset 2: Municipal Daily Data  
- **Current**: `daily_municipal_historical_forecast.csv.gz`
- **Coverage**: Historical through 7-day forecast
- **Sources**: Station data aggregated to municipal + AEMET forecasts

### Dataset 3: Hourly Station Data
- **Current**: `hourly_station_recent.csv.gz`
- **Coverage**: Building historical archive from recent collections
- **Sources**: AEMET current observations endpoint

## Alternative Naming Options:

**Option A - Temporal Coverage:**
- `daily_station_historical.csv.gz`
- `daily_municipal_extended.csv.gz` (historical + forecast)
- `hourly_station_ongoing.csv.gz`

**Option B - Data Type:**
- `daily_station_archive.csv.gz`
- `daily_municipal_complete.csv.gz` (past + future)
- `hourly_station_realtime.csv.gz`

**Option C - Comprehensive:**
- `daily_station_historical_present.csv.gz`
- `daily_municipal_historical_forecast.csv.gz`
- `hourly_station_accumulating.csv.gz`
