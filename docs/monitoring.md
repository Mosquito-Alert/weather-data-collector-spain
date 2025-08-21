# Weather Data Collector - Monitoring Guide

## Dashboard Integration

This project is integrated with the [mosquito-alert-model-monitor](https://github.com/Mosquito-Alert/mosquito-alert-model-monitor) dashboard for comprehensive monitoring of weather data collection jobs.

## Monitored Components

### Job Types

| Job Name | Description | Schedule | Priority | SLURM Script |
|----------|------------|----------|----------|--------------|
| `weather-forecast` | Municipal forecasts collection | Every 6 hours | CRITICAL | `priority_municipal_forecast.sh` |
| `weather-hourly` | Hourly station observations | Every 2 hours | MEDIUM | Included in `update_weather.sh` |
| `weather-historical` | Historical data updates | Daily (3 AM) | LOW | `update_historical_weather.sh` |
| `municipal-forecast-priority` | Immediate municipal data | On-demand | CRITICAL | `priority_municipal_forecast.sh` |
| `weather-data-collector` | Complete collection pipeline | Every 6 hours | HIGH | `update_weather.sh` |

### Metrics Tracked

- **Job Status**: running, completed, failed
- **Progress**: 0-100% completion
- **Duration**: Execution time in seconds
- **Resource Usage**: CPU and memory utilization
- **Data Metrics**: Output file count and size
- **API Health**: Rate limits, errors, response times

### Alert Conditions

- **API Rate Limits**: >3 rate limit hits
- **Empty Responses**: >5 consecutive empty server responses
- **Stale Data**: Forecast data >12 hours old
- **Disk Space**: >90% disk usage in output directory
- **Failed Jobs**: Any job failing repeatedly

## Setup Instructions

### 1. Clone Monitor Repository

```bash
# On cluster, in research directory
cd ~/research
git clone https://github.com/Mosquito-Alert/mosquito-alert-model-monitor.git
cd mosquito-alert-model-monitor

# Setup monitoring environment
conda env create -f environment.yml
conda activate mosquito-alert-monitor
```

### 2. Test Integration

```bash
# From weather project directory
./scripts/test_dashboard_integration.sh
```

### 3. Deploy Dashboard

```bash
# Local rendering
cd mosquito-alert-model-monitor
quarto preview index.qmd

# GitHub Pages deployment (automatic on push)
git add .
git commit -m "Add weather monitoring integration"
git push origin main
```

## Status File Format

Each weather job creates/updates a JSON status file in the monitor repository:

```json
{
  "job_name": "weather-forecast",
  "status": "running",
  "last_updated": "2025-08-21T18:30:00Z",
  "duration": 1800,
  "progress": 75,
  "cpu_usage": 85.2,
  "memory_usage": 2048,
  "next_scheduled_run": "2025-08-22T00:30:00Z",
  "config": {
    "project_type": "weather_data_collection",
    "data_source": "AEMET OpenData API",
    "collection_scope": "Spain",
    "municipalities": 8129,
    "api_keys": 3,
    "output_datasets": 3
  },
  "metrics": {
    "output_files": 3,
    "total_size_mb": 245.6
  },
  "alerts": {
    "api_errors": false,
    "disk_space_low": false,
    "rate_limit_exceeded": false
  }
}
```

## Dashboard Views

### Main Dashboard
- Real-time status of all weather jobs
- Resource usage charts
- Recent activity timeline
- Alert notifications

### Job Details
- Individual job logs and configuration
- Performance trends over time
- Error analysis and troubleshooting

### Historical Analytics
- Success rates by job type
- Performance trends
- Data collection statistics

## Troubleshooting

### Common Issues

1. **Status Files Not Appearing**
   - Check monitor repository path in scripts
   - Verify write permissions on status directory
   - Ensure status script is executable

2. **Dashboard Not Updating**
   - Check JSON syntax in status files
   - Verify Quarto rendering process
   - Check GitHub Pages deployment

3. **Missing Resource Metrics**
   - Verify `ps` command availability
   - Check script permissions for system monitoring

### Log Files

Monitor these log files for debugging:

- `logs/weather_collection_*.out` - Main collection pipeline
- `logs/municipal_priority_*.out` - Priority municipal data
- `logs/get_forecast_data_*.out` - Forecast collection
- `logs/get_latest_data_*.out` - Hourly observations
- `logs/get_historical_data_*.out` - Historical updates

## Integration Commands

```bash
# Manual status update
./scripts/update_weather_status.sh "job_name" "status" duration progress

# Test dashboard integration
./scripts/test_dashboard_integration.sh

# Deploy with monitoring
./deploy_to_cluster.sh dashboard
```
