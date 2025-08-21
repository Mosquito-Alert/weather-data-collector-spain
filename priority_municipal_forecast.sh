#!/bin/bash
# filepath: /Users/palmer/research/weather-data-collector-spain/priority_municipal_forecast.sh
#SBATCH --job-name=municipal-forecast-priority
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/municipal_forecast_priority_%j.out
#SBATCH --error=logs/municipal_forecast_priority_%j.err

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0

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

# Create logs directory
mkdir -p logs

# Initialize status reporting
JOB_NAME="municipal-forecast-priority"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

# Report job started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 0

# Activate renv
R --slave --no-restore --file=- <<EOF
renv::activate()
EOF

echo "Starting priority municipal data generation: $(date)"

# Get forecasts first (immediate availability)
echo "Collecting municipal forecasts for immediate model use..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 30
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/priority_forecast_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 60
    echo "✅ Forecast collection successful"
else
    $STATUS_SCRIPT "$JOB_NAME" "failed" $(($(date +%s) - START_TIME)) 30
    echo "❌ Forecast collection failed"
    exit 1
fi

# Generate backwards municipal data
echo "Generating municipal data backwards from present..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 80
R CMD BATCH --no-save --no-restore code/generate_municipal_priority.R logs/priority_municipal_$(date +%Y%m%d_%H%M%S).out

FINAL_DURATION=$(($(date +%s) - START_TIME))

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "$JOB_NAME" "completed" $FINAL_DURATION 100
    echo "✅ Priority municipal data generation completed: $(date)"
    echo "Models can now use: data/output/daily_municipal_extended.csv.gz"
else
    $STATUS_SCRIPT "$JOB_NAME" "failed" $FINAL_DURATION 80
    echo "❌ Municipal data generation failed"
    exit 1
fi

# Submit: sbatch priority_municipal_forecast.sh
