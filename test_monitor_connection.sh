#!/bin/bash

# Test script to verify monitor dashboard connectivity

echo "=== Testing Monitor Dashboard Connection ==="

MONITOR_REPO="$HOME/research/mosquito-alert-model-monitor"
STATUS_SCRIPT="./scripts/update_weather_status.sh"

echo "Monitor repository path: $MONITOR_REPO"
echo "Weather repo path: $(pwd)"

# Check if monitor repo exists
if [ -d "$MONITOR_REPO" ]; then
    echo "✅ Monitor repository found"
    echo "Contents:"
    ls -la "$MONITOR_REPO" | head -5
    echo ""
    
    # Check status directory
    STATUS_DIR="$MONITOR_REPO/data/status"
    if [ -d "$STATUS_DIR" ]; then
        echo "✅ Status directory exists: $STATUS_DIR"
        echo "Permissions: $(ls -ld "$STATUS_DIR")"
    else
        echo "⚠️  Status directory not found, will create: $STATUS_DIR"
        mkdir -p "$STATUS_DIR"
        if [ -d "$STATUS_DIR" ]; then
            echo "✅ Status directory created successfully"
        else
            echo "❌ Failed to create status directory"
        fi
    fi
    echo ""
else
    echo "❌ Monitor repository not found at: $MONITOR_REPO"
    echo "Available repositories in research:"
    ls -la "$HOME/research/" | grep "^d" | head -5
    echo ""
fi

# Test status script
echo "Testing status script..."
if [ -f "$STATUS_SCRIPT" ]; then
    echo "✅ Status script found: $STATUS_SCRIPT"
    echo "Making test status call..."
    $STATUS_SCRIPT "test-connection" "running" 0 50
    echo ""
    
    # Check if status file was created
    if [ -f "$MONITOR_REPO/data/status/test-connection.json" ]; then
        echo "✅ Status file created successfully!"
        echo "Status file content:"
        head -10 "$MONITOR_REPO/data/status/test-connection.json"
        echo ""
        echo "File size: $(stat -c%s "$MONITOR_REPO/data/status/test-connection.json") bytes"
    else
        echo "❌ Status file was not created"
    fi
else
    echo "❌ Status script not found: $STATUS_SCRIPT"
fi

echo ""
echo "=== Monitor Connection Test Complete ==="
