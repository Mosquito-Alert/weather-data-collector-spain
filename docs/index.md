---
title: Spanish Weather Data Collection System
layout: default
---

# Spanish Weather Data Collection System

Automated collection and processing of Spanish meteorological data from AEMET OpenData API.

## System Overview

This system collects three standardized weather datasets covering all Spanish weather stations and municipalities. Data is automatically processed, quality-controlled, and delivered as analysis-ready CSV files.

**Performance**: Complete data collection in 2-4 hours (48x improvement over previous methods)  
**Coverage**: 4,000+ weather stations, 8,000+ municipalities across Spain  
**Automation**: Daily collection with automatic gap filling and quality control

## Data Products

### Daily Station Historical Data
**File**: `daily_station_historical.csv`

Daily meteorological measurements from Spanish weather stations.

| Variable | Description | Units |
|----------|-------------|-------|
| date | Observation date | YYYY-MM-DD |
| station_id | AEMET station identifier | string |
| temp_mean | Daily mean temperature | °C |
| temp_max | Daily maximum temperature | °C |
| temp_min | Daily minimum temperature | °C |
| precipitation | Daily precipitation | mm |
| humidity_mean | Daily mean relative humidity | % |
| wind_speed | Daily mean wind speed | km/h |
| pressure_max | Daily maximum pressure | hPa |

**Coverage**: 4,000+ stations across Spain  
**Update Frequency**: Daily at 2:00 AM

### Daily Municipal Extended Data  
**File**: `daily_municipal_extended.csv`

Municipal-level weather data combining forecasts with station aggregations.

| Variable | Description | Units |
|----------|-------------|-------|
| date | Date of observation/forecast | YYYY-MM-DD |
| municipality_id | CUMUN municipality code | string |
| temp_mean | Daily mean temperature | °C |
| temp_max | Daily maximum temperature | °C |
| temp_min | Daily minimum temperature | °C |
| humidity_mean | Daily mean relative humidity | % |
| wind_speed | Daily mean wind speed | km/h |
| data_source | Source type | 'station_aggregated' or 'forecast' |

**Coverage**: 8,000+ Spanish municipalities  
**Data Priority**: Station aggregations replace forecasts when available  
**Update Frequency**: Daily at 2:00 AM

### Hourly Station Ongoing Data
**File**: `hourly_station_ongoing.csv`

High-frequency station measurements for detailed temporal analysis.

| Variable | Description | Units |
|----------|-------------|-------|
| datetime | Observation timestamp | ISO datetime |
| station_id | AEMET station identifier | string |
| variable_type | Measurement type | string |
| value | Measured value | varies |

**Coverage**: Selected weather stations  
**Update Frequency**: Daily at 2:00 AM

## Data Flow Architecture

```
┌─────────────────┐
│ AEMET OpenData  │
│     API         │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Data Collection │
│  (R scripts)    │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Quality Control │
│ & Validation    │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Standardization │
│ & Aggregation   │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Final Datasets  │
│   (CSV files)   │
└─────────────────┘
```

## Technical Implementation

### Performance Optimization
- **climaemet Package**: Provides 48x speedup for municipal forecast collection
- **Parallel Processing**: Batch processing for station data collection
- **Rate Limiting**: Automatic API throttling to respect AEMET limits
- **Incremental Updates**: Only collect new data to minimize processing time

### Quality Control
- **Temperature Validation**: Realistic range checks for Spanish climate
- **Precipitation Filtering**: Remove impossible precipitation values  
- **Completeness Checks**: Flag incomplete observation periods
- **Duplicate Detection**: Prevent data duplication across collection runs

### Data Standardization
- **Variable Names**: Consistent English names across all datasets
- **Units**: Standardized metric units throughout
- **Date Formats**: ISO 8601 date formatting
- **Municipality Codes**: CUMUN format with documented conversion mappings

### Gap Management
- **Automatic Detection**: Weekly analysis identifies missing data periods
- **Intelligent Filling**: Targeted collection without redundant downloads
- **Priority System**: Recent data prioritized over historical gaps
- **Tracking**: Prevent repeated attempts for permanently unavailable data

## Installation and Setup

### Prerequisites
- SLURM HPC environment
- R 4.4.2 with GDAL 3.10.0
- AEMET OpenData API key

### Required R Packages
```r
install.packages(c("tidyverse", "climaemet", "meteospain", "data.table", "lubridate"))
```

### Configuration
1. **API Key Setup**: Store AEMET API key in `auth/keys.R`
2. **Module Loading**: Ensure SLURM environment loads required modules
3. **Directory Structure**: Verify `data/output/` and `logs/` directories exist

### Automation Setup
Add to crontab for automated operation:
```bash
# Daily data collection (2:00 AM)
0 2 * * * cd /path/to/project && sbatch scripts/bash/update_weather_hybrid.sh

# Daily documentation updates (6:00 AM)  
0 6 * * * cd /path/to/project && sbatch scripts/bash/update_readme_summary.sh

# Weekly gap filling (Sunday 1:00 AM)
0 1 * * 0 cd /path/to/project && sbatch scripts/bash/fill_gaps.sh
```

## Usage

### Manual Data Collection
```bash
# Full data collection
sbatch scripts/bash/update_weather_hybrid.sh

# Gap analysis and filling
sbatch scripts/bash/fill_gaps.sh

# Documentation update
sbatch scripts/bash/update_readme_summary.sh
```

### Monitoring
- **Log Files**: Check `logs/` directory for SLURM job outputs
- **Data Summary**: Automatic README updates show current collection status
- **Error Handling**: Failed jobs logged with detailed error messages

## File Organization

```
weather-data-collector-spain/
├── scripts/
│   ├── r/              # R collection and analysis scripts
│   ├── bash/           # SLURM job scripts
│   └── archive/        # Archived scripts
├── data/
│   ├── output/         # Final standardized datasets
│   ├── backup/         # Data backups
│   └── input/          # Reference data
├── docs/               # Documentation (this site)
├── auth/               # API credentials (git-ignored)
└── logs/               # SLURM job logs
```

## Variable Reference

### Original AEMET to Standardized Mapping

| AEMET Variable | Standard Name | Description |
|----------------|---------------|-------------|
| tmed | temp_mean | Daily mean temperature |
| tmax | temp_max | Daily maximum temperature |
| tmin | temp_min | Daily minimum temperature |
| prec | precipitation | Daily precipitation |
| hrMedia | humidity_mean | Daily mean relative humidity |
| velmedia | wind_speed | Daily mean wind speed |
| indicativo | station_id | Station identifier |
| municipio_id | municipality_id | Municipality identifier (CUMUN) |

### Municipality Code Information
**Format**: CUMUN codes from AEMET municipal forecast system  
**Structure**: 5-digit numeric codes  
**Coverage**: All Spanish municipalities (~8,000)  
**Note**: Different from INE codes - conversion required for administrative data merges

## Performance Metrics

### Collection Times
- **Station Daily Data**: ~15 minutes for full collection
- **Municipal Forecasts**: ~5 minutes (vs 5+ hours previously)
- **Hourly Data**: ~10 minutes for recent period
- **Total Runtime**: 2-4 hours (vs 33+ hours with original approach)

### Data Volumes
- **Daily Station**: ~700KB, 4,500+ records daily
- **Municipal Extended**: ~2.5MB, 18,000+ records daily
- **Hourly Ongoing**: Variable based on collection period

## Support

For technical questions or issues:
1. Check log files in `logs/` directory
2. Review error messages in SLURM job outputs
3. Verify API key configuration in `auth/keys.R`
4. Ensure all required R packages are installed

## License

MIT License - see LICENSE file for complete terms.
- [Variable Reference](variables.html) - Complete variable definitions

## Recent Updates

**August 2025**: Major expansion from 5 to 7 core variables with comprehensive forecast integration.
