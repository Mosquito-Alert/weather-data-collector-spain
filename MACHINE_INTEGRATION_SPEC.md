# MACHINE_INTEGRATION_SPEC.md
# Project Integration Specification for mosquito-alert-model-monitor
# This file contains all information needed to integrate any project with the dashboard
# Written for AI assistant to understand and implement without errors

## INTEGRATION_METADATA
```yaml
dashboard_repo: "mosquito-alert-model-monitor"
dashboard_path: "$HOME/research/mosquito-alert-model-monitor"
integration_version: "3.0_bulletproof"
compatible_with: ["bash", "python", "R", "SLURM"]
git_conflict_resilient: true
```

## ARCHITECTURE_PRINCIPLES

### SEPARATION_OF_CONCERNS
- **Main projects**: Focus on their core mission, fail fast on infrastructure issues
- **Monitor project**: Handle all dashboard complexity, never fail calling jobs  
- **Status integration**: Simple drop-in calls, always exit 0

### ROBUSTNESS_HIERARCHY
1. **Core data collection NEVER fails due to dashboard issues**
2. **Status updates are best-effort only**
3. **Git conflicts handled gracefully without blocking jobs**
4. **Module loading is required for main scripts, not defensive**

## CORRECT_INTEGRATION_PATTERN

### FOR_MAIN_PROJECT_SCRIPTS
```bash
# DO THIS: Simple, focused scripts that do their job
#!/bin/bash
#SBATCH --job-name=my-job

# Load required modules (MUST succeed or job should fail)
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0

# Set up job
JOB_NAME="my-job-name"
STATUS_SCRIPT="./scripts/update_weather_status.sh"  # Drop-in wrapper
START_TIME=$(date +%s)

# Status updates (never fail)
$STATUS_SCRIPT "$JOB_NAME" "running" 0 5

# Do the actual work
echo "Starting main work..."
python my_main_script.py

# Final status
$STATUS_SCRIPT "$JOB_NAME" "completed" $(($(date +%s) - START_TIME)) 100
```

### FOR_PROJECT_STATUS_WRAPPER
```bash
#!/bin/bash
# Drop-in status update script: scripts/update_weather_status.sh
# This script NEVER fails the calling job

JOB_NAME="${1:-project-unknown}"
STATUS="${2:-unknown}"  
DURATION="${3:-0}"
PROGRESS="${4:-0}"
LOG_MESSAGE="${5:-Job status update}"

# Use the robust monitor script if available
MONITOR_SCRIPT="$HOME/research/mosquito-alert-model-monitor/scripts/update_job_status.sh"

if [ -f "$MONITOR_SCRIPT" ]; then
    echo "📊 Updating dashboard via monitor project..."
    "$MONITOR_SCRIPT" "$JOB_NAME" "$STATUS" "$DURATION" "$PROGRESS" "$LOG_MESSAGE"
else
    echo "⚠️  Monitor project not found - skipping dashboard update"
fi

# ALWAYS exit successfully so calling jobs continue
exit 0
```
~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh "PROJECT_JOB_NAME" "running" $ELAPSED_SECONDS $PROGRESS_PERCENT "Current step description"

# Job completion (success)
~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh "PROJECT_JOB_NAME" "completed" $TOTAL_SECONDS 100 "Job completed successfully"

# Job failure (in error handling)
~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh "PROJECT_JOB_NAME" "failed" $ELAPSED_SECONDS $PROGRESS_PERCENT "Error: description"
```

### STEP_3_LOG_INTEGRATION
```bash
# OPTIONAL: Add at end of main script for log collection
~/research/mosquito-alert-model-monitor/scripts/collect_logs.sh "" "PROJECT_NAME" "PROJECT_LOG_DIR_PATH"
```

### STEP_4_VARIABLES_TO_REPLACE
```yaml
replacements_needed:
  PROJECT_JOB_NAME: 
    description: "Main identifier for this job in dashboard"
    examples: ["prepare_malert_data", "weather-forecast", "model_training"]
    format: "lowercase_with_underscores_or_hyphens"
    
  PROJECT_NAME:
    description: "Short project identifier"
    examples: ["mosquito_model_data_prep", "weather", "ml_pipeline"]
    
  PROJECT_LOG_DIR_PATH:
    description: "Absolute path to project log directory"
    examples: ["$HOME/research/project_name/logs", "./logs", "/path/to/logs"]
    
  ELAPSED_SECONDS:
    description: "Time since job start"
    calculation: "$(($(date +%s) - $START_TIME))"
    
  PROGRESS_PERCENT:
    description: "Job completion percentage (0-100)"
    examples: [0, 25, 50, 75, 100]
    
  TOTAL_SECONDS:
    description: "Total job duration at completion"
    calculation: "$(($(date +%s) - $START_TIME))"
```

## INTEGRATION_TEMPLATES

### BASH_SCRIPT_TEMPLATE
```bash
#!/bin/bash
# Add at beginning of main script:

# Dashboard integration setup
DASHBOARD_SCRIPT="$HOME/research/mosquito-alert-model-monitor/scripts/update_job_status.sh"
JOB_NAME="PROJECT_JOB_NAME"  # REPLACE WITH ACTUAL JOB NAME
START_TIME=$(date +%s)

# Job start notification
$DASHBOARD_SCRIPT "$JOB_NAME" "running" 0 0 "Starting PROJECT_DESCRIPTION"

# Add throughout script for progress:
# $DASHBOARD_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - $START_TIME)) PROGRESS_PERCENT "STEP_DESCRIPTION"

# Example progress calls:
$DASHBOARD_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - $START_TIME)) 25 "Data loading complete"
$DASHBOARD_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - $START_TIME)) 50 "Processing data"
$DASHBOARD_SCRIPT "$JOB_NAME" "running" $(($(date +%s) - $START_TIME)) 75 "Generating outputs"

# At end of script:
$DASHBOARD_SCRIPT "$JOB_NAME" "completed" $(($(date +%s) - $START_TIME)) 100 "Job completed successfully"

# Optional log collection:
$HOME/research/mosquito-alert-model-monitor/scripts/collect_logs.sh "" "PROJECT_NAME" "./logs"
```

### PYTHON_SCRIPT_TEMPLATE
```python
import subprocess
import time
import sys

# Dashboard integration
DASHBOARD_SCRIPT = os.path.expanduser("~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh")
JOB_NAME = "PROJECT_JOB_NAME"  # REPLACE WITH ACTUAL JOB NAME
start_time = time.time()

def update_status(status, progress, message):
    """Update job status in dashboard"""
    elapsed = int(time.time() - start_time)
    try:
        subprocess.run([DASHBOARD_SCRIPT, JOB_NAME, status, str(elapsed), str(progress), message], 
                      check=False, capture_output=True)
    except:
        pass  # Never fail the main job due to dashboard issues

# Job start
update_status("running", 0, "Starting PROJECT_DESCRIPTION")

# Progress updates throughout code:
update_status("running", 25, "Data loading complete")
update_status("running", 50, "Processing data")
update_status("running", 75, "Generating outputs")

# Job completion
update_status("completed", 100, "Job completed successfully")
```

### R_SCRIPT_TEMPLATE
```r
# Dashboard integration for R scripts
dashboard_script <- "~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh"
job_name <- "PROJECT_JOB_NAME"  # REPLACE WITH ACTUAL JOB NAME
start_time <- Sys.time()

update_status <- function(status, progress, message) {
  elapsed <- as.integer(difftime(Sys.time(), start_time, units = "secs"))
  tryCatch({
    system(paste(dashboard_script, job_name, status, elapsed, progress, shQuote(message)), 
           ignore.stdout = TRUE, ignore.stderr = TRUE)
  }, error = function(e) {
    # Never fail the main job due to dashboard issues
  })
}

# Job start
update_status("running", 0, "Starting PROJECT_DESCRIPTION")

# Progress updates throughout code:
update_status("running", 25, "Data loading complete")
update_status("running", 50, "Processing data") 
update_status("running", 75, "Generating outputs")

# Job completion
update_status("completed", 100, "Job completed successfully")
```

## SLURM_INTEGRATION
```bash
# Add to SLURM script headers:
#SBATCH --job-name=PROJECT_JOB_NAME

# Add after SLURM setup, before main work:
DASHBOARD_SCRIPT="$HOME/research/mosquito-alert-model-monitor/scripts/update_job_status.sh"
JOB_NAME="PROJECT_JOB_NAME"
START_TIME=$(date +%s)

$DASHBOARD_SCRIPT "$JOB_NAME" "running" 0 0 "SLURM job started (ID: $SLURM_JOB_ID)"

# Add before exit:
$DASHBOARD_SCRIPT "$JOB_NAME" "completed" $(($(date +%s) - $START_TIME)) 100 "SLURM job completed (ID: $SLURM_JOB_ID)"
```

## ERROR_HANDLING_PATTERN
```bash
# Robust error handling that doesn't break jobs
set +e  # Don't exit on dashboard errors

# Wrap main job logic
main_job_function() {
    # Your original job code here
    return $?
}

# Call main job with error handling
if main_job_function; then
    $DASHBOARD_SCRIPT "$JOB_NAME" "completed" $(($(date +%s) - $START_TIME)) 100 "Job completed successfully"
else
    $DASHBOARD_SCRIPT "$JOB_NAME" "failed" $(($(date +%s) - $START_TIME)) 50 "Job failed with error"
    exit 1
fi
```

## TESTING_CHECKLIST
```yaml
verify_integration:
  - status_file_created: "ls -la ~/research/mosquito-alert-model-monitor/data/status/PROJECT_JOB_NAME.json"
  - test_script_manually: "./scripts/test_dashboard_integration.sh"
  - check_dashboard_locally: "open ~/research/mosquito-alert-model-monitor/docs/index.html"
  - verify_no_job_failures: "Run original job and ensure it completes even if dashboard fails"
```

## COMMON_PROJECT_TYPES

### DAILY_DATA_PROCESSING
```yaml
typical_pattern:
  job_name: "project_daily_process"
  schedule: "Daily via cron"
  stages: ["download", "process", "upload", "cleanup"]
  progress_points: [0, 25, 50, 75, 100]
```

### MODEL_TRAINING
```yaml
typical_pattern:
  job_name: "model_training"
  schedule: "Weekly/Monthly"
  stages: ["data_prep", "training", "validation", "deployment"]
  progress_points: [0, 20, 60, 90, 100]
```

### DATA_COLLECTION
```yaml
typical_pattern:
  job_name: "data_collection"
  schedule: "Hourly/Daily"
  stages: ["fetch", "validate", "store", "backup"]
  progress_points: [0, 30, 70, 100]
```

## IMPLEMENTATION_NOTES
- All scripts exit with code 0 to prevent job failures
- Dashboard updates are "best effort" - job success is priority
- JSON status files use standardized format
- Log collection is optional and safe
- Git operations have timeouts and retries
- SLURM jobs get proper resource allocation
- No dependencies on external libraries
