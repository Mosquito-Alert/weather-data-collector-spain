#!/bin/bash
#SBATCH --job-name=weather-daily
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=6G
#SBATCH --time=02:00:00
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/weather_daily_%j.out
#SBATCH --error=logs/weather_daily_%j.err

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3

# Set working directory
cd ~/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

# Create output directory if it doesn't exist
mkdir -p data/output

# Initialize status reporting
JOB_NAME="weather-daily"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

# Report job started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 5

echo "Starting daily weather data collection: $(date)"

# Municipal forecasts
echo "Collecting municipal forecasts..."
$STATUS_SCRIPT "weather-forecast" "running" $(($(date +%s) - START_TIME)) 25
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/get_forecast_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-forecast" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-forecast" "failed" $(($(date +%s) - START_TIME)) 30
fi

# Hourly observations
echo "Collecting hourly observations..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 50
R CMD BATCH --no-save --no-restore code/get_latest_data.R logs/get_latest_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-hourly" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-hourly" "failed" $(($(date +%s) - START_TIME)) 50
fi

# Generate aggregated datasets
echo "Generating aggregated datasets..."
$STATUS_SCRIPT "weather-aggregation" "running" $(($(date +%s) - START_TIME)) 90
./generate_all_datasets.sh

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-aggregation" "completed" $(($(date +%s) - START_TIME)) 100
    echo "✅ All datasets generated successfully"
else
    $STATUS_SCRIPT "weather-aggregation" "failed" $(($(date +%s) - START_TIME)) 90
    echo "❌ Dataset generation failed"
fi

echo "Daily weather data collection completed: $(date)"

# Final status update
FINAL_DURATION=$(($(date +%s) - START_TIME))
$STATUS_SCRIPT "$JOB_NAME" "completed" $FINAL_DURATION 100
