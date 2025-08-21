#!/bin/bash
# filepath: /Users/palmer/research/weather-data-collector-spain/update_weather.sh
#SBATCH --job-name=weather-collect
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/weather_collection_%j.out
#SBATCH --error=logs/weather_collection_%j.err

# Load required modules
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0
module load openssl/1.1.1

# Set working directory
cd ~/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

# Initialize status reporting
JOB_NAME="weather-data-collector"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

# Report job started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 0

# Initialize renv if first run
if [ ! -f "renv.lock" ]; then
    echo "Initializing renv..."
    $STATUS_SCRIPT "$JOB_NAME" "running" 0 10
    R --slave --no-restore --file=- <<EOF
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos="https://cran.r-project.org")
}
renv::init()
renv::install(c("tidyverse", "lubridate", "data.table", "curl", "jsonlite", "httr", "R.utils"))
renv::snapshot()
EOF
fi

# Activate renv
R --slave --no-restore --file=- <<EOF
renv::activate()
EOF

# Pull any pending commits
git pull origin main

echo "Starting weather data collection: $(date)"
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 20

# Priority 1: Municipal forecasts (immediate model needs)
echo "Collecting municipal forecasts..."
$STATUS_SCRIPT "weather-forecast" "running" $(($(date +%s) - START_TIME)) 30
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/get_forecast_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-forecast" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-forecast" "failed" $(($(date +%s) - START_TIME)) 30
fi

# Priority 2: Generate municipal priority data (backwards from present)
echo "Generating priority municipal data..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 50
R CMD BATCH --no-save --no-restore code/generate_municipal_priority.R logs/generate_municipal_priority_$(date +%Y%m%d_%H%M%S).out

# Priority 3: Hourly observations
echo "Collecting hourly observations..."
$STATUS_SCRIPT "weather-hourly" "running" $(($(date +%s) - START_TIME)) 70
R CMD BATCH --no-save --no-restore code/get_latest_data.R logs/get_latest_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-hourly" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-hourly" "failed" $(($(date +%s) - START_TIME)) 70
fi

# Priority 4: Historical data update
echo "Updating historical data..."
$STATUS_SCRIPT "weather-historical" "running" $(($(date +%s) - START_TIME)) 90
R CMD BATCH --no-save --no-restore code/get_historical_data.R logs/get_historical_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-historical" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-historical" "failed" $(($(date +%s) - START_TIME)) 90
fi

echo "Weather data collection completed: $(date)"

# Final status update
FINAL_DURATION=$(($(date +%s) - START_TIME))
$STATUS_SCRIPT "$JOB_NAME" "completed" $FINAL_DURATION 100

# Commit and push results (optional - uncomment if you want to track outputs)
# git add data/output/*.csv.gz logs/*.out
# git commit -m "Weather data update: $(date +%Y-%m-%d_%H:%M:%S) (cluster - automated)"
# git pull origin main
# git push origin main

# Submit: sbatch update_weather.sh