#!/bin/bash

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

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load Miniconda3/24.7.1-0

# Activate conda environment
conda activate mosquito-alert-monitor


# scripts/update_weather_status.sh
# ---------------------------------
# Integration script for mosquito-alert-model-monitor dashboard
# Reports weather data collection job status

# Configuration
MONITOR_REPO_PATH="$HOME/research/mosquito-alert-model-monitor"  # Path to monitor dashboard
STATUS_DIR="$MONITOR_REPO_PATH/data/status"
JOB_NAME="${1:-weather-data-collector}"
STATUS="${2:-unknown}"
DURATION="${3:-0}"
PROGRESS="${4:-0}"

# Check if monitor repository exists
if [ ! -d "$MONITOR_REPO_PATH" ]; then
    echo "⚠️  Monitor repository not found at: $MONITOR_REPO_PATH"
    echo "Status update skipped - monitor not available"
    exit 0
fi

# Ensure status directory exists
mkdir -p "$STATUS_DIR"

# Check if status directory is writable
if [ ! -w "$STATUS_DIR" ]; then
    echo "⚠️  Cannot write to status directory: $STATUS_DIR"
    echo "Status update skipped - no write permissions"
    exit 0
fi

# Get current timestamp
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Calculate CPU and memory usage
CPU_USAGE=$(ps aux | awk '{cpu += $3} END {print cpu}' 2>/dev/null || echo "0")
MEMORY_USAGE=$(ps aux | awk '{mem += $4} END {print mem*1024/100}' 2>/dev/null || echo "0")

# Determine next scheduled run based on job type
case "$JOB_NAME" in
    "weather-forecast")
        NEXT_RUN=$(date -d '+6 hours' -u +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    "weather-hourly")
        NEXT_RUN=$(date -d '+2 hours' -u +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    "weather-historical")
        NEXT_RUN=$(date -d '+1 day' -u +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    *)
        NEXT_RUN=$(date -d '+6 hours' -u +"%Y-%m-%dT%H:%M:%SZ")
        ;;
esac

# Get latest log entries from the project logs
get_latest_logs() {
    local log_file="logs/${JOB_NAME}_$(date +%Y%m%d)*.out"
    if [ -f $log_file ]; then
        tail -n 3 $log_file 2>/dev/null | jq -R . | jq -s .
    else
        echo '["Starting weather data collection", "Initializing API connections", "Processing requests..."]'
    fi
}

# Get data file information
get_data_info() {
    local output_dir="data/output"
    local file_count=0
    local total_size=0
    
    if [ -d "$output_dir" ]; then
        file_count=$(find "$output_dir" -name "*.csv.gz" | wc -l)
        total_size=$(find "$output_dir" -name "*.csv.gz" -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum/1024/1024}' || echo "0")
    fi
    
    echo "{\"output_files\": $file_count, \"total_size_mb\": ${total_size:-0}}"
}

# Build status JSON matching monitor format
cat > "$STATUS_DIR/${JOB_NAME}.json" << EOF
{
  "job_name": "$JOB_NAME",
  "status": "$STATUS",
  "last_updated": "$TIMESTAMP",
  "start_time": "$TIMESTAMP",
  "duration": ${DURATION:-0},
  "progress": ${PROGRESS:-0},
  "cpu_usage": $CPU_USAGE,
  "memory_usage": $MEMORY_USAGE,
  "next_scheduled_run": "$NEXT_RUN",
  "config": {
    "project_type": "weather_data_collection",
    "data_source": "AEMET OpenData API",
    "collection_scope": "Spain",
    "script": "$(basename "$0")",
    "cluster": "SLURM cluster"
  }
}
EOF

echo "Status updated for $JOB_NAME: $STATUS"
echo "Status file written to: $STATUS_DIR/${JOB_NAME}.json"
echo "Monitor repo: $MONITOR_REPO_PATH"

# Verify the file was written successfully
if [ -f "$STATUS_DIR/${JOB_NAME}.json" ]; then
    echo "✅ Status file created successfully"
    echo "File size: $(stat -c%s "$STATUS_DIR/${JOB_NAME}.json" 2>/dev/null || echo "unknown") bytes"
else
    echo "❌ Failed to create status file"
fi

# Optional: Trigger dashboard update if running locally
if [ -f "$MONITOR_REPO_PATH/index.qmd" ] && [ "$AUTO_RENDER" = "true" ]; then
    cd "$MONITOR_REPO_PATH"
    quarto render index.qmd >/dev/null 2>&1 &
fi
