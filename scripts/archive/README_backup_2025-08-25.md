# Weather Data Collector - Spain

This repository provides scripts to download, update, and manage weather data from AEMET weather stations across Spain, producing three comprehensive datasets for analysis and research.

## 📊 Current Data Collection Status

*This section is automatically updated daily with current dataset statistics.*

---

## Three Output Datasets

### Dataset 1: Daily Station Data
**File**: `daily_station_aggregated_YYYY-MM-DD.csv.gz`

Daily aggregated weather data by station:
- Data sources: AEMET daily climatological endpoint + hourly observations aggregated to daily
- Variables: daily min/max/mean temperature, precipitation, wind, humidity, pressure
- Coverage: Active weather stations across Spain
- Quality control: Temperature range validation, realistic value bounds

### Dataset 2: Municipal Daily Data  
**File**: `municipal_aggregated_YYYY-MM-DD.csv.gz`

Daily weather data by municipality:
- Data sources: Station data aggregated by municipality + 7-day municipal forecasts
- Coverage: 8,129 Spanish municipalities
- Temporal range: Historical station aggregates through 7-day forecasts
- Source tracking: Distinguishes between station-derived data and forecast data

### Dataset 3: Hourly Station Data
**File**: `hourly_station_ongoing.csv.gz`

Hourly observations from AEMET stations:
- Data format: Long format (measure/value pairs) for 7 core variables
- Update frequency: Daily collection with continuous archiving
- Purpose: Building comprehensive historical hourly archive

## Data Collection System

### Collection Methods
- **Station Daily Data**: Custom API calls to AEMET climatological endpoints
- **Municipal Forecasts**: `climaemet` R package for robust API interaction
- **Hourly Data**: Direct API calls to AEMET observational endpoints

### Automation
- **Schedule**: Daily collection via SLURM batch system
- **Gap Detection**: Automated identification of missing data
- **Gap Filling**: Weekly targeted collection for missing records
- **Quality Control**: Automated validation of temperature ranges and data consistency

### Data Processing Pipeline

```mermaid
flowchart TD
    A[AEMET API] --> B[Station Daily Endpoint]
    A --> C[Hourly Observations] 
    A --> D[Municipal Forecasts]
    
    B --> E[get_station_daily_hybrid.R]
    C --> F[get_latest_data.R]
    D --> G[get_forecast_data_hybrid.R]
    
    E --> H[Station Daily Data]
    F --> I[Hourly Archive]
    G --> J[Municipal Forecasts]
    
    H --> K[aggregate_daily_station_data_hybrid.R]
    I --> K
    K --> L[Dataset 1: Daily Station Aggregated]
    
    L --> M[aggregate_municipal_data_hybrid.R]
    J --> M
    M --> N[Dataset 2: Municipal Aggregated]
    
    I --> O[Dataset 3: Hourly Archive]
```

## File Structure

### Core Collection Scripts
```
code/
├── get_station_daily_hybrid.R        # Station daily data collection
├── get_forecast_data_hybrid.R        # Municipal forecast collection  
├── get_latest_data.R                 # Hourly data collection
├── collect_all_datasets_hybrid.R     # Coordinated collection of all datasets
├── aggregate_daily_station_data_hybrid.R  # Station data aggregation
└── aggregate_municipal_data_hybrid.R      # Municipal data aggregation
```

### Data Quality & Monitoring
```
code/
├── check_data_gaps.R                 # Gap detection and analysis
├── fill_data_gaps.R                  # Targeted gap filling
├── generate_data_summary.R           # Dataset statistics generation
└── update_readme_with_summary.R      # Automated documentation updates
```

### SLURM Integration
```
├── update_weather_hybrid.sh          # Main collection job
└── CRONTAB_LINES_TO_ADD.txt         # Scheduling configuration
```

## Setup and Usage

### Prerequisites
```bash
# Load required modules (HPC environment)
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Install required R packages
Rscript -e "install.packages(c('climaemet', 'tidyverse', 'data.table', 'lubridate'))"
```

### Manual Collection
```bash
# Collect all three datasets
sbatch update_weather_hybrid.sh

# Or run individual components
Rscript code/get_forecast_data_hybrid.R      # Municipal forecasts
Rscript code/get_station_daily_hybrid.R      # Station daily data  
Rscript code/get_latest_data.R               # Hourly data
```

### Automated Schedule
Add these lines to your crontab:
```bash
# Daily collection (2 AM)
0 2 * * * cd /path/to/weather-data-collector-spain && sbatch update_weather_hybrid.sh

# Daily status update (6 AM)  
0 6 * * * cd /path/to/weather-data-collector-spain && Rscript code/update_readme_with_summary.R

# Weekly gap filling (Sunday 1 AM)
0 1 * * 0 cd /path/to/weather-data-collector-spain && Rscript code/fill_data_gaps.R
```

## Data Quality

### Coverage and Success Rates
- **Station Daily**: Typical success rates of 30-50% per collection run (normal for AEMET API)
- **Municipal Forecasts**: 65-95% success rates with automatic retry logic
- **Hourly Data**: >99% success rate for active stations

### Quality Control Measures
- **Temperature validation**: Range checks (min ≤ mean ≤ max, realistic bounds)
- **Duplicate handling**: Automatic deduplication with source prioritization
- **Gap tracking**: Systematic identification and filling of missing data
- **Source attribution**: Clear distinction between observed vs forecast data

## Data Access

### Output Location
All datasets are saved in `data/output/` with date-stamped filenames:
- `daily_station_aggregated_YYYY-MM-DD.csv[.gz]`
- `municipal_aggregated_YYYY-MM-DD.csv[.gz]`  
- `hourly_station_ongoing.csv.gz`

### File Formats
- **CSV format**: Compatible with R, Python, and standard analysis tools
- **Compressed versions**: `.gz` files for efficient storage
- **Consistent schemas**: Standardized column names across collection runs

## Monitoring and Maintenance

### Gap Analysis
```bash
# Check for missing data
Rscript code/check_data_gaps.R

# Fill identified gaps
Rscript code/fill_data_gaps.R
```

### Data Statistics
```bash
# Generate current dataset summary
Rscript code/generate_data_summary.R

# Update README with latest statistics  
Rscript code/update_readme_with_summary.R
```

### Error Handling
- **API Rate Limits**: Automatic detection and waiting
- **SSL Connection Issues**: Built-in retry logic in `climaemet` package
- **Server Errors**: Exponential backoff for temporary failures
- **Missing Data**: Systematic gap detection and targeted re-collection

## Technical Details

### API Integration
- **AEMET OpenData API**: Primary data source requiring valid API key
- **Rate Limiting**: Respectful API usage with automatic throttling
- **Error Recovery**: Robust handling of temporary failures and connection issues

### Dependencies
- **R Packages**: `climaemet`, `tidyverse`, `data.table`, `lubridate`, `httr`, `jsonlite`
- **System Requirements**: GDAL/3.10.0, R/4.4.2
- **Environment**: HPC cluster with SLURM job scheduler

### Performance
- **Collection Time**: Typically 2-4 hours for complete daily collection
- **Resource Usage**: 8GB RAM, single CPU core sufficient
- **Storage Growth**: Approximately 50-100MB per day across all datasets

---

*This system provides reliable, automated collection of comprehensive weather data for Spain with built-in quality control and gap management.*

## Features
- **Real-time Observations**: Fetches current hourly weather from all AEMET stations
- **Historical Data**: Updates and maintains daily historical weather dataset
- **Forecast Collection**: Downloads 7-day municipal forecasts for all 8,129 Spanish municipalities
- **Variable Compatibility**: Uses consistent variables across observation, historical, and forecast data
- **Robust Error Handling**: API rate limits, timeouts, and errors managed with retry logic
- **Concurrent Run Prevention**: Configurable lockfile system prevents script conflicts
- **Data Compression**: All outputs stored in efficient CSV.gz format

## Requirements
- R (recommended version 4.0 or higher)
- API key for AEMET OpenData (see below)
- R packages: tidyverse, lubridate, data.table, curl, jsonlite, RSocrata, R.utils

## Setup
1. **API Key**: Obtain an API key from [AEMET OpenData](https://opendata.aemet.es/centrodedescargas/inicio).
2. **Auth Directory**: Place your API key as `my_api_key` in a file called `keys.R` inside an untracked `auth/` directory:
   ```r
   my_api_key <- "YOUR_API_KEY_HERE"
   ```
3. **Install Dependencies**: Install required R packages if not already present.

## Usage

### Current Weather Observations
- Run `code/get_latest_data_expanded.R` to fetch latest observations with expanded variable set (7 safe variables)
- Original script `code/get_latest_data.R` available for basic 5-variable collection
- Recommended frequency: every 2 hours

### Historical Weather Data  
- Run `code/get_historical_data.R` to update historical daily weather dataset
- Run as needed to maintain historical records

### Forecast Data Collection
- Run `code/get_forecast_data_simple.R` for robust 7-day municipal forecasts (recommended)
- Alternative: `code/get_forecast_data.R` (original enhanced version)
- Configure `SAMPLE_SIZE` for testing (e.g., 20) or `NULL` for all 8,129 municipalities
- See `docs/forecast-collection.md` for detailed configuration

### Data Analysis
- `code/variable_compatibility_analysis.R` - analyzes variable compatibility across endpoints
- `code/aggregate_daily_station_data.R` - processes daily aggregations
- `code/aggregate_municipal_data.R` - municipal-level data processing

All output files are written to the `data/` directory as compressed CSVs.

## Directory Structure
```
weather-data-collector-spain/
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
