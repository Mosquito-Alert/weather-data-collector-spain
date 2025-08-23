# MACHINE_INTEGRATION_SPEC.md
# Project Integration Specification for mosquito-alert-model-monitor
# This file contains all information needed to integrate any project with the dashboard
# Written for AI assistant to understand and implement without errors

## INTEGRATION_METADATA
```yaml
dashboard_repo: "mosquito-alert-model-monitor"
dashboard_path: "$HOME/research/mosquito-alert-model-monitor"
integration_version: "2.0_robust"
compatible_with: ["bash", "python", "R", "SLURM"]
```

## REQUIRED_FILES_IN_DASHBOARD
```yaml
scripts_needed:
  - "scripts/update_job_status.sh"           # Main status update (robust version)
  - "scripts/update_job_status_and_push.sh"  # Alternative name (same file)
  - "scripts/collect_logs.sh"                # Log collection
  - "scripts/slurm_dashboard_sync.sh"        # SLURM cron sync
  
directories_needed:
  - "data/status/"    # Status JSON files
  - "data/history/"   # Historical job data  
  - "data/details/"   # Log excerpts
  - "logs/"          # Dashboard sync logs
```

## PROJECT_INTEGRATION_PATTERN

### STEP_1_IDENTIFICATION
```yaml
identify_in_project:
  main_script_patterns: ["*.sh", "main.py", "run_*.py", "process_*.sh"]
  existing_status_calls: 
    - "./scripts/update_job_status.sh"
    - "../mosquito-alert-model-monitor/scripts/update_job_status*.sh"
  log_directories: ["logs/", "output/", "log/"]
  schedule_info: ["crontab", "*.sh", "README.md", "slurm_*.sh"]
```

### STEP_2_STATUS_INTEGRATION
```bash
# REQUIRED: Replace or add these calls in main project script
# PATTERN: Call at start, middle (progress updates), and end

# Job start
~/research/mosquito-alert-model-monitor/scripts/update_job_status.sh "PROJECT_JOB_NAME" "running" 0 0 "Job started"

# Progress updates (throughout script)
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
