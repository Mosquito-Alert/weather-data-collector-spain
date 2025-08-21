#!/bin/bash
#SBATCH --job-name=priority-sequence
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/priority_sequence_%j.out
#SBATCH --error=logs/priority_sequence_%j.err

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0
module load Miniconda3/24.7.1-0

# Activate conda environment
conda activate mosquito-alert-monitor

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

# Create necessary directories
mkdir -p logs
mkdir -p data/output

# Initialize status reporting
JOB_NAME="priority-sequence"
STATUS_SCRIPT="./scripts/update_weather_status.sh"
START_TIME=$(date +%s)

echo "=== PRIORITY DATA COLLECTION SEQUENCE ==="
echo "Started at: $(date)"

# Report sequence started
$STATUS_SCRIPT "$JOB_NAME" "running" 0 0

# Step 1: Municipal Forecasts (immediate priority)
echo ""
echo "Step 1/2: Collecting Municipal Forecasts..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 20

# Activate renv
R --slave --no-restore --file=- <<EOF
renv::activate()
EOF

echo "Running forecast collection..."
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/priority_forecasts_$(date +%Y%m%d_%H%M%S).out

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 50
    echo "✅ Forecast collection completed successfully"
    echo "Waiting 30 seconds before next step..."
    sleep 30
else
    $STATUS_SCRIPT "$JOB_NAME" "failed" $(($(date +%s) - START_TIME)) 20
    echo "❌ Forecast collection failed - stopping sequence"
    exit 1
fi

# Step 2: Municipal Priority Data (historical backfill)
echo ""
echo "Step 2/2: Generating Municipal Priority Data..."
$STATUS_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - START_TIME)) 70

echo "Running municipal data generation..."
R CMD BATCH --no-save --no-restore code/aggregate_municipal_data.R logs/priority_municipal_$(date +%Y%m%d_%H%M%S).out

FINAL_DURATION=$(($(date +%s) - START_TIME))

if [ $? -eq 0 ]; then
    $STATUS_SCRIPT "$JOB_NAME" "completed" $FINAL_DURATION 100
    echo "✅ Priority sequence completed successfully: $(date)"
    echo ""
    echo "Generated datasets:"
    echo "- Municipal forecasts: data/output/municipal_forecasts_$(date +%Y-%m-%d).csv"
    echo "- Municipal aggregated data: Available for model use"
    echo ""
    echo "Total duration: ${FINAL_DURATION} seconds"
else
    $STATUS_SCRIPT "$JOB_NAME" "failed" $FINAL_DURATION 70
    echo "❌ Municipal data generation failed"
    exit 1
fi

echo "=== PRIORITY SEQUENCE COMPLETE ==="
