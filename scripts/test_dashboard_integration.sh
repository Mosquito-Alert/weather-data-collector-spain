#!/bin/bash

# test_dashboard_integration.sh
# -----------------------------
# Test script to verify dashboard integration is working

# Configuration
MONITOR_REPO="$HOME/mosquito-alert-model-monitor"  # Adjust path as needed
WEATHER_REPO="$HOME/research/weather-data-collector-spain"

echo "=== Weather Data Collector - Dashboard Integration Test ==="

# Check if monitor repo exists
if [ ! -d "$MONITOR_REPO" ]; then
    echo "❌ Monitor repository not found at: $MONITOR_REPO"
    echo "Please clone: git clone https://github.com/Mosquito-Alert/mosquito-alert-model-monitor.git"
    exit 1
fi

# Create status directory in monitor repo
mkdir -p "$MONITOR_REPO/data/status"

echo "✅ Monitor repository found"

# Test status script
cd "$WEATHER_REPO"
echo "Testing status reporting..."

# Test different job statuses
./scripts/update_weather_status.sh "weather-forecast" "running" 120 45
./scripts/update_weather_status.sh "weather-hourly" "completed" 300 100
./scripts/update_weather_status.sh "weather-historical" "failed" 180 25
./scripts/update_weather_status.sh "municipal-forecast-priority" "running" 90 75

echo "✅ Status files created:"
ls -la "$MONITOR_REPO/data/status/weather-*.json"

# Verify JSON structure
echo ""
echo "Sample status file content:"
cat "$MONITOR_REPO/data/status/weather-forecast.json" | jq .

# Test dashboard rendering (if quarto is available)
if command -v quarto &> /dev/null; then
    echo ""
    echo "Testing dashboard rendering..."
    cd "$MONITOR_REPO"
    
    # Check if the dashboard can see our weather jobs
    if quarto render index.qmd --quiet; then
        echo "✅ Dashboard renders successfully"
        echo "🌐 Open: $MONITOR_REPO/docs/index.html"
    else
        echo "⚠️  Dashboard rendering failed - check dependencies"
    fi
else
    echo "⚠️  Quarto not found - cannot test dashboard rendering"
fi

echo ""
echo "=== Integration Test Complete ==="
echo ""
echo "🎯 Your weather jobs should now appear in the monitoring dashboard"
echo "📊 Dashboard URL: file://$MONITOR_REPO/docs/index.html"
echo "🔄 Status files: $MONITOR_REPO/data/status/"
echo ""
echo "Next steps:"
echo "1. Check the dashboard shows your weather jobs"
echo "2. Run weather collection scripts to see live updates"
echo "3. Deploy dashboard to GitHub Pages for remote monitoring"
