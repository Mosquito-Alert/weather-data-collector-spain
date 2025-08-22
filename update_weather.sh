#!/bin/bash
#SBATCH --job-name=weather-collect
#SBATCH --partition=ceab
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
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3
module load Miniconda3/24.7.1-0

# Initialize and activate conda environment
source ~/.bashrc  # Ensure conda is initialized
if ! conda activate mosquito-alert-monitor; then
    echo "WARNING: Failed to activate conda environment mosquito-alert-monitor"
    echo "Continuing with default environment..."
fi

# Load SSH agent since this is no longer done by default on the cluster
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

# Set locale environment variables
export LC_CTYPE=C.UTF-8
export LC_COLLATE=C.UTF-8
export LC_TIME=C.UTF-8
export LC_MESSAGES=C.UTF-8
export LC_MONETARY=C.UTF-8
export LC_PAPER=C.UTF-8
export LC_MEASUREMENT=C.UTF-8
export LANG=C.UTF-8

# Set working directory
cd ~/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

# Create output directory if it doesn't exist
mkdir -p data/output

# Initialize status reporting
JOB_NAME="weather-hourly"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

# Report job started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 5

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
else
    echo "Restoring renv packages..."
    $STATUS_SCRIPT "$JOB_NAME" "running" 0 15
    R --slave --no-restore --file=- <<EOF
renv::restore(prompt = FALSE)
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
$STATUS_SCRIPT "weather-forecast" "running" $(($(date +%s) - START_TIME)) 25
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/get_forecast_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-forecast" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-forecast" "failed" $(($(date +%s) - START_TIME)) 30
fi

# Priority 2: Hourly observations
echo "Collecting hourly observations..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 50
R CMD BATCH --no-save --no-restore code/get_latest_data.R logs/get_latest_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-hourly" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-hourly" "failed" $(($(date +%s) - START_TIME)) 50
fi

# Priority 3: Historical data update
echo "Updating historical data..."
$STATUS_SCRIPT "weather-historical" "running" $(($(date +%s) - START_TIME)) 70
R CMD BATCH --no-save --no-restore code/get_historical_data.R logs/get_historical_data_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-historical" "completed" $(($(date +%s) - START_TIME)) 100
else
    $STATUS_SCRIPT "weather-historical" "failed" $(($(date +%s) - START_TIME)) 70
fi

# Priority 4: Generate aggregated datasets
echo "Generating aggregated datasets..."
$STATUS_SCRIPT "weather-aggregation" "running" $(($(date +%s) - START_TIME)) 90

# Run dataset aggregation
./generate_all_datasets.sh

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "weather-aggregation" "completed" $(($(date +%s) - START_TIME)) 100
    echo "✅ All datasets generated successfully"
else
    $STATUS_SCRIPT "weather-aggregation" "failed" $(($(date +%s) - START_TIME)) 90
    echo "❌ Dataset generation failed"
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