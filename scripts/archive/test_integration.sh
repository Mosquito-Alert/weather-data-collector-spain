#!/bin/bash

# Test script to verify weather-data-collector-spain integration with dashboard monitoring
# Run this from the weather-data-collector-spain directory

echo "=== Testing Weather Data Collector Dashboard Integration ==="

# Check if we're in the right directory
if [ ! -f "README.md" ] || [ ! -d "scripts" ]; then
    echo "❌ Please run this script from the weather-data-collector-spain directory"
    exit 1
fi

# Check if monitor dashboard exists
MONITOR_REPO="$HOME/research/mosquito-alert-model-monitor"
if [ ! -d "$MONITOR_REPO" ]; then
    echo "❌ Monitor dashboard not found at: $MONITOR_REPO"
    echo "Please ensure the mosquito-alert-model-monitor repo is cloned at the expected location"
    exit 1
fi

echo "✅ Found monitor dashboard at: $MONITOR_REPO"

# Test status update script
echo ""
echo "Testing status update functionality..."

# Test each weather job type
./scripts/update_weather_status.sh "weather-forecast" "running" 120 45
echo "✅ Updated weather-forecast status"

./scripts/update_weather_status.sh "weather-hourly" "completed" 300 100  
echo "✅ Updated weather-hourly status"

./scripts/update_weather_status.sh "weather-historical" "running" 180 25
echo "✅ Updated weather-historical status"

./scripts/update_weather_status.sh "municipal-forecast-priority" "completed" 90 100
echo "✅ Updated municipal-forecast-priority status"

# Check if status files were created
echo ""
echo "Verifying status files..."
for job in weather-forecast weather-hourly weather-historical municipal-forecast-priority; do
    if [ -f "$MONITOR_REPO/data/status/${job}.json" ]; then
        echo "✅ ${job}.json created successfully"
    else
        echo "❌ ${job}.json not found"
    fi
done

# Display sample status file
echo ""
echo "Sample status file content:"
echo "=========================="
cat "$MONITOR_REPO/data/status/weather-forecast.json" | jq .

echo ""
echo "=== Integration Test Complete ==="
echo ""
echo "🎯 Weather data collection jobs are now integrated with the monitoring dashboard"
echo "📊 Dashboard location: $MONITOR_REPO/docs/index.html"
echo "🔄 Status updates will automatically trigger dashboard rebuilds via git push"
echo ""
echo "Next steps:"
echo "1. Run a weather collection job to see live updates"
echo "2. Check the dashboard shows the weather jobs with their statuses"
echo "3. Deploy the dashboard to GitHub Pages for remote monitoring"
