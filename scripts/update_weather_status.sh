#!/bin/bash

# scripts/update_weather_status.sh
# ---------------------------------
# Integration script for mosquito-alert-model-monitor dashboard
# Reports weather data collection job status

# Configuration
MONITOR_REPO_PATH="$HOME/mosquito-alert-model-monitor"  # Adjust path as needed
STATUS_DIR="$MONITOR_REPO_PATH/data/status"
JOB_NAME="${1:-weather-data-collector}"
STATUS="${2:-unknown}"
DURATION="${3:-0}"
PROGRESS="${4:-0}"

# Ensure status directory exists
mkdir -p "$STATUS_DIR"

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

# Build comprehensive status JSON
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
  "log_entries": $(get_latest_logs),
  "config": {
    "project_type": "weather_data_collection",
    "data_source": "AEMET OpenData API",
    "collection_scope": "Spain",
    "municipalities": 8129,
    "api_keys": 3,
    "output_datasets": 3
  },
  "metrics": $(get_data_info),
  "alerts": {
    "api_errors": false,
    "disk_space_low": false,
    "rate_limit_exceeded": false
  }
}
EOF

echo "Status updated for $JOB_NAME: $STATUS"

# Optional: Trigger dashboard update if running locally
if [ -f "$MONITOR_REPO_PATH/index.qmd" ] && [ "$AUTO_RENDER" = "true" ]; then
    cd "$MONITOR_REPO_PATH"
    quarto render index.qmd >/dev/null 2>&1 &
fi
