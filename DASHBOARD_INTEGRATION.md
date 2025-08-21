# Weather Data Collector - Dashboard Integration

## Overview

The weather-data-collector-spain project is now fully integrated with the mosquito-alert-model-monitor dashboard for real-time job monitoring and status tracking.

## Monitored Jobs

The dashboard tracks four critical weather data collection jobs:

### 🌦️ **weather-forecast** (CRITICAL PRIORITY)
- **Purpose**: Municipal forecast collection for 8,129 Spanish municipalities
- **Frequency**: Every 6 hours  
- **Script**: `priority_municipal_forecast.sh`
- **Data Output**: 7-day municipal forecasts
- **Critical for**: Model predictions requiring immediate forecast data

### ⏰ **weather-hourly** (MEDIUM PRIORITY)
- **Purpose**: Hourly station observations from all AEMET stations
- **Frequency**: Every 2 hours
- **Script**: `update_weather.sh`
- **Data Output**: Real-time weather observations
- **Critical for**: Current conditions and short-term analysis

### 📊 **weather-historical** (LOW PRIORITY)
- **Purpose**: Daily historical weather data backfill
- **Frequency**: Daily (typically overnight)
- **Script**: `update_historical_weather.sh` 
- **Data Output**: Historical daily climatology
- **Critical for**: Long-term trend analysis and model training

### 🚀 **municipal-forecast-priority** (CRITICAL PRIORITY)
- **Purpose**: Immediate municipal data generation for model deployment
- **Frequency**: On-demand/immediate needs
- **Script**: `priority_municipal_data.sh`
- **Data Output**: Complete municipal dataset with forecasts
- **Critical for**: Urgent model deployment needs

## How It Works

### 1. **Status Reporting**
Each weather collection script uses `scripts/update_weather_status.sh` to report:
- Job status (running, completed, failed, waiting)
- Execution duration and progress percentage
- Resource usage (CPU, memory)
- Next scheduled run time
- Detailed configuration and metadata

### 2. **Automatic Dashboard Updates**
When jobs update their status:
1. Status JSON file is updated in the monitor repository
2. Changes are automatically committed to git
3. Git push triggers GitHub Actions workflow
4. Dashboard is rebuilt and deployed within 2-3 minutes
5. Live dashboard shows updated job statuses

### 3. **Status File Format**
Each job creates a JSON status file with this structure:
```json
{
  "job_name": "weather-forecast",
  "status": "completed",
  "last_updated": "2025-08-22T14:30:00Z",
  "start_time": "2025-08-22T14:25:00Z",
  "duration": 300,
  "progress": 100,
  "cpu_usage": 45.2,
  "memory_usage": 1024,
  "next_scheduled_run": "2025-08-22T20:30:00Z",
  "config": {
    "project_type": "weather_data_collection",
    "data_source": "AEMET OpenData API",
    "collection_scope": "Municipal forecasts",
    "frequency": "Every 6 hours",
    "priority": "CRITICAL"
  }
}
```

## Testing Integration

### Run Integration Test
```bash
cd ~/research/weather-data-collector-spain
./test_integration.sh
```

### Manual Status Update
```bash
./scripts/update_weather_status.sh "weather-forecast" "running" 120 75
```

### View Dashboard
- **Local**: `~/research/mosquito-alert-model-monitor/docs/index.html`
- **Live**: [GitHub Pages URL when deployed]

## HPC Cluster Usage

### SLURM Job Examples
```bash
# Submit priority municipal forecast job
sbatch priority_municipal_forecast.sh

# Submit regular weather update job  
sbatch update_weather.sh

# Submit historical data update
sbatch update_historical_weather.sh
```

### Cron Job Examples
```bash
# Every 6 hours: Municipal forecasts (critical)
0 */6 * * * cd ~/research/weather-data-collector-spain && sbatch priority_municipal_forecast.sh

# Every 2 hours: Station observations  
0 */2 * * * cd ~/research/weather-data-collector-spain && sbatch update_weather.sh

# Daily: Historical data update
0 6 * * * cd ~/research/weather-data-collector-spain && sbatch update_historical_weather.sh
```

## Dashboard Benefits

1. **Real-time Monitoring**: See job status, progress, and performance metrics
2. **Failure Detection**: Immediate notification when jobs fail
3. **Resource Tracking**: Monitor CPU, memory usage across jobs
4. **Schedule Visibility**: Know when jobs are due to run next
5. **Historical Trends**: Track job performance over time
6. **Remote Access**: Monitor jobs from anywhere via GitHub Pages

## Troubleshooting

### Common Issues

**Status not updating:**
- Check git push permissions from HPC cluster
- Verify monitor repository path in scripts
- Ensure conda environment is activated

**Dashboard not rendering:**
- Check GitHub Actions workflow status
- Verify Quarto and R dependencies
- Check for JSON syntax errors in status files

**Jobs not appearing:**
- Ensure status files are created in correct directory
- Check file permissions
- Verify JSON structure matches expected format

### Debug Commands
```bash
# Check if status files exist
ls -la ~/research/mosquito-alert-model-monitor/data/status/

# Validate JSON syntax
jq . ~/research/mosquito-alert-model-monitor/data/status/weather-forecast.json

# Test git operations
cd ~/research/mosquito-alert-model-monitor
git status
git log --oneline -5
```

## Integration Maintenance

- **Status scripts** are automatically updated with each job run
- **Dashboard rebuilds** happen automatically via GitHub Actions
- **No manual intervention** required for normal operations
- **Monitor git repository** should be kept up to date with latest changes

This integration provides comprehensive monitoring for all weather data collection activities, ensuring reliable operation and immediate visibility into job status and performance.
