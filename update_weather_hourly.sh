#!/bin/bash
#SBATCH --job-name=weather-hourly
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/weather_hourly_%j.out
#SBATCH --error=logs/weather_hourly_%j.err

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3

# Set working directory
cd ~/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

# Create output directory if it doesn't exist
mkdir -p data/output

# Initialize status reporting
JOB_NAME="weather-hourly-quick"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

# Report job started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 5

echo "Starting hourly weather data collection: $(date)"

# Only collect current hourly observations
echo "Collecting hourly observations..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 50
R CMD BATCH --no-save --no-restore code/get_latest_data.R logs/get_latest_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "$JOB_NAME" "completed" $(($(date +%s) - START_TIME)) 100
    echo "✅ Hourly data collection successful"
else
    $STATUS_SCRIPT "$JOB_NAME" "failed" $(($(date +%s) - START_TIME)) 50
    echo "❌ Hourly data collection failed"
fi

echo "Hourly weather data collection completed: $(date)"

# Final status update
FINAL_DURATION=$(($(date +%s) - START_TIME))
$STATUS_SCRIPT "$JOB_NAME" "completed" $FINAL_DURATION 100
