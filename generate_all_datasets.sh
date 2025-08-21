#!/bin/bash

# generate_all_datasets.sh
# ------------------------
# Purpose: Run all 3 aggregation scripts to generate the required datasets
#
# This script executes the aggregation functions in the correct order to produce:
# 1. Dataset 1: Daily station data (historical + current aggregated)
# 2. Dataset 2: Daily municipal data (observations + forecasts) 
# 3. Dataset 3: Hourly station data (already produced by get_latest_data.R)
#
# Usage: ./generate_all_datasets.sh
#
# Dependencies: 
#   - Historical data: data/output/daily_station_historical.csv.gz
#   - Current hourly data: data/output/hourly_station_ongoing.csv.gz  
#   - Municipal forecasts: data/output/municipal_forecasts_*.csv
#   - Municipality reference: data/input/municipalities.csv.gz
#
# Author: John Palmer
# Date: 2025-08-22

set -e  # Exit on any error

# Set working directory to script location
cd "$(dirname "$0")"

# Create necessary directories
mkdir -p logs
mkdir -p data/output

# Set up logging
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_PREFIX="logs/generate_datasets_${TIMESTAMP}"

echo "=== GENERATING ALL REQUIRED DATASETS ==="
echo "Started at: $(date)"
echo "Log files will be saved with prefix: ${LOG_PREFIX}"

# Check if R is available
if ! command -v R &> /dev/null; then
    echo "ERROR: R is not available. Please install R or load the R module."
    exit 1
fi

# Check for required input files
echo ""
echo "Checking for required input files..."

REQUIRED_FILES=(
    "data/output/hourly_station_ongoing.csv.gz"
    "data/input/municipalities.csv.gz"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        MISSING_FILES+=("$file")
    fi
done

if [[ ${#MISSING_FILES[@]} -gt 0 ]]; then
    echo "ERROR: Missing required files:"
    printf '  - %s\n' "${MISSING_FILES[@]}"
    echo ""
    echo "Please run the data collection scripts first:"
    echo "  - ./update_weather.sh (for hourly data)"
    echo "  - Rscript code/get_forecast_data.R (for forecasts)"
    exit 1
fi

# Check for historical data (optional but recommended)
if [[ ! -f "data/output/daily_station_historical.csv.gz" ]]; then
    echo "WARNING: Historical daily station data not found."
    echo "         Dataset 1 and 2 will only include recent data."
    echo "         Run 'Rscript code/get_historical_data.R' to get historical data."
fi

# Check for forecast data (optional for Dataset 2)
FORECAST_FILES=(data/output/municipal_forecasts_*.csv)
if [[ ! -f ${FORECAST_FILES[0]} ]]; then
    echo "WARNING: No municipal forecast files found."
    echo "         Dataset 2 will only include observations."
    echo "         Run 'Rscript code/get_forecast_data.R' to get forecasts."
fi

echo "Input file check complete."

# Step 1: Generate Dataset 1 - Daily Station Data
echo ""
echo "=== STEP 1/3: GENERATING DATASET 1 (Daily Station Data) ==="
echo "Running aggregate_daily_station_data.R..."

R CMD BATCH --no-save --no-restore code/aggregate_daily_station_data.R "${LOG_PREFIX}_dataset1.out"

if [[ $? -eq 0 ]]; then
    echo "✅ Dataset 1 generation completed successfully"
    if [[ -f "data/output/daily_station_aggregated.csv.gz" ]]; then
        echo "   Output: data/output/daily_station_aggregated.csv.gz"
        echo "   Size: $(du -h data/output/daily_station_aggregated.csv.gz | cut -f1)"
    fi
else
    echo "❌ Dataset 1 generation failed"
    echo "   Check log file: ${LOG_PREFIX}_dataset1.out"
    exit 1
fi

# Step 2: Generate Dataset 2 - Municipal Daily Data (observations + forecasts)
echo ""
echo "=== STEP 2/3: GENERATING DATASET 2 (Municipal Daily Data) ==="
echo "Running aggregate_municipal_daily.R..."

R CMD BATCH --no-save --no-restore code/aggregate_municipal_daily.R "${LOG_PREFIX}_dataset2.out"

if [[ $? -eq 0 ]]; then
    echo "✅ Dataset 2 generation completed successfully"
    if [[ -f "data/output/daily_municipal_extended.csv.gz" ]]; then
        echo "   Output: data/output/daily_municipal_extended.csv.gz"
        echo "   Size: $(du -h data/output/daily_municipal_extended.csv.gz | cut -f1)"
    fi
else
    echo "❌ Dataset 2 generation failed"
    echo "   Check log file: ${LOG_PREFIX}_dataset2.out"
    exit 1
fi

# Step 3: Verify Dataset 3 - Hourly Station Data (should already exist)
echo ""
echo "=== STEP 3/3: VERIFYING DATASET 3 (Hourly Station Data) ==="

if [[ -f "data/output/hourly_station_ongoing.csv.gz" ]]; then
    echo "✅ Dataset 3 already exists and is up to date"
    echo "   Output: data/output/hourly_station_ongoing.csv.gz"
    echo "   Size: $(du -h data/output/hourly_station_ongoing.csv.gz | cut -f1)"
    echo "   (This dataset is maintained by get_latest_data.R)"
else
    echo "❌ Dataset 3 missing"
    echo "   Run './update_weather.sh' to generate hourly station data"
fi

echo ""
echo "=== ALL DATASETS GENERATION COMPLETE ==="
echo "Completed at: $(date)"
echo ""
echo "Generated datasets:"
echo "1. Daily station data: data/output/daily_station_aggregated.csv.gz"
echo "2. Municipal daily data: data/output/daily_municipal_extended.csv.gz"  
echo "3. Hourly station data: data/output/hourly_station_ongoing.csv.gz"
echo ""
echo "Log files:"
echo "- Dataset 1: ${LOG_PREFIX}_dataset1.out"
echo "- Dataset 2: ${LOG_PREFIX}_dataset2.out"
