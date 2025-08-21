#!/bin/bash

# deploy_to_cluster.sh
# --------------------
# Deployment script for cluster setup and immediate municipal data generation

echo "=== Weather Data Collector - Cluster Deployment ==="
echo "Started at: $(date)"

# Configuration
CLUSTER_USER="your_username"
CLUSTER_HOST="your_cluster.edu"
CLUSTER_PATH="/path/to/weather-data-collector-spain"
LOCAL_PATH="/Users/palmer/research/weather-data-collector-spain"

# Function to deploy code to cluster
deploy_code() {
    echo "=== Deploying code to cluster ==="
    
    # Sync code (excluding data outputs)
    rsync -avz --exclude='data/output/' --exclude='.git/' \
          "$LOCAL_PATH/" "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_PATH/"
    
    echo "✅ Code deployment completed"
}

# Function to setup secure API keys
setup_api_keys() {
    echo "=== Setting up secure API keys ==="
    
    # Copy API keys with secure permissions
    scp "$LOCAL_PATH/auth/keys.R" "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_PATH/auth/"
    
    # Set secure permissions on cluster
    ssh "$CLUSTER_USER@$CLUSTER_HOST" "chmod 700 $CLUSTER_PATH/auth && chmod 600 $CLUSTER_PATH/auth/keys.R"
    
    echo "✅ API keys secured on cluster"
}

# Function to install R dependencies
install_dependencies() {
    echo "=== Installing R dependencies ==="
    
    ssh "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_PATH && Rscript -e '
    packages <- c(\"tidyverse\", \"lubridate\", \"data.table\", \"curl\", \"jsonlite\", \"httr\")
    install.packages(packages[!packages %in% installed.packages()[,1]], repos=\"https://cran.r-project.org\")
    '"
    
    echo "✅ R dependencies installed"
}

# Function to start priority municipal data generation
start_priority_generation() {
    echo "=== Starting priority municipal data generation ==="
    
    # First, collect latest forecast data
    ssh "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_PATH && Rscript code/get_forecast_data.R" &
    
    # Start priority municipal generation (forecast first)
    ssh "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_PATH && nohup Rscript code/generate_municipal_priority.R forecast > logs/municipal_priority.log 2>&1 &"
    
    echo "✅ Priority generation started - check logs/municipal_priority.log"
}

# Function to setup dashboard integration
setup_dashboard_integration() {
    echo "=== Setting up dashboard integration ==="
    
    # Check if monitor repo exists on cluster
    if ssh "$CLUSTER_USER@$CLUSTER_HOST" "[ -d mosquito-alert-model-monitor ]"; then
        echo "✅ Monitor repository found on cluster"
        
        # Ensure status directory exists
        ssh "$CLUSTER_USER@$CLUSTER_HOST" "mkdir -p mosquito-alert-model-monitor/data/status"
        
        # Test status reporting
        ssh "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_PATH && ./scripts/update_weather_status.sh weather-test running 0 0"
        
        echo "✅ Dashboard integration configured"
    else
        echo "⚠️  Monitor repository not found - clone it manually:"
        echo "   git clone https://github.com/Mosquito-Alert/mosquito-alert-model-monitor.git"
    fi
}
setup_cron_jobs() {
    echo "=== Setting up cron jobs ==="
    
    # Create crontab entries
    ssh "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_PATH && cat > cluster_crontab.txt << 'EOF'
# Weather Data Collection - Priority Schedule
# Municipal forecasts (every 6 hours) - PRIORITY for models
0 */6 * * * cd $CLUSTER_PATH && Rscript code/get_forecast_data.R >> logs/forecast_cron.log 2>&1

# Municipal data generation (after forecast collection)
30 */6 * * * cd $CLUSTER_PATH && Rscript code/generate_municipal_priority.R forecast >> logs/municipal_cron.log 2>&1

# Hourly station data (every 2 hours) 
0 */2 * * * cd $CLUSTER_PATH && Rscript code/get_latest_data.R >> logs/hourly_cron.log 2>&1

# Historical data updates (daily at 3 AM)
0 3 * * * cd $CLUSTER_PATH && Rscript code/get_historical_data.R >> logs/historical_cron.log 2>&1

# Historical municipal chunks (daily at 4 AM) 
0 4 * * * cd $CLUSTER_PATH && Rscript code/generate_municipal_priority.R historical >> logs/historical_municipal_cron.log 2>&1
EOF"
    
    echo "📅 Cron jobs configured - install with: crontab cluster_crontab.txt"
}

# Main deployment sequence
main() {
    case "${1:-all}" in
        "code")
            deploy_code
            ;;
        "keys") 
            setup_api_keys
            ;;
        "deps")
            install_dependencies
            ;;
        "start")
            start_priority_generation
            ;;
        "dashboard")
            setup_dashboard_integration
            ;;
        "cron")
            setup_cron_jobs
            ;;
        "all")
            deploy_code
            setup_api_keys
            install_dependencies
            setup_dashboard_integration
            setup_cron_jobs
            start_priority_generation
            ;;
        *)
            echo "Usage: $0 [code|keys|deps|dashboard|start|cron|all]"
            exit 1
            ;;
    esac
}

# Run with provided argument or default to 'all'
main "$1"

echo "=== Cluster deployment completed at: $(date) ==="
echo ""
echo "🎯 PRIORITY DATA FOR MODELS:"
echo "   Municipal forecast data: data/output/daily_municipal_forecast_only.csv.gz"
echo "   Full municipal dataset: data/output/daily_municipal_extended.csv.gz"
echo ""
echo "📊 Data will be updated every 6 hours automatically"
echo "📝 Monitor progress: tail -f logs/municipal_priority.log"
