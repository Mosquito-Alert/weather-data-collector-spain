#!/bin/bash
#SBATCH --job-name=weather_hybrid
#SBATCH --output=logs/weather_hybrid_%j.out
#SBATCH --error=logs/weather_hybrid_%j.err
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --partition=ceab

# HYBRID WEATHER DATA COLLECTION SLURM JOB
# Collects all three required datasets using optimal approaches:
# 1. Station daily data (custom API calls - proven)
# 2. Municipal forecasts (climaemet package - 48x faster)
# 3. Hourly data (existing working approach)

echo "=== SLURM Job Information ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Node: $HOSTNAME"
echo "Started at: $(date)"
echo ""

# Load required modules
echo "Loading modules..."
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3

# Verify modules loaded
echo "R version: $(R --version | head -1)"
echo "GDAL version: $(gdal-config --version)"
echo ""

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

# Run the consolidated collection (three-file strategy)
echo "Starting consolidated weather data collection..."
echo "Strategy: Append to three final files (no fragmentation)"
echo "Expected completion time: 2-4 hours (vs 33+ hours with old approach)"
echo ""

# Execute the standardized collection script
Rscript code/collect_all_datasets_standardized.R

exit_code=$?

echo ""
echo "=== Job Completion ==="
echo "Exit code: $exit_code"
echo "Completed at: $(date)"

if [ $exit_code -eq 0 ]; then
    echo "✅ Hybrid collection completed successfully"
    
    # Show output file sizes
    echo ""
    echo "Generated files:"
    for file in data/output/*$(date +%Y-%m-%d)*; do
        if [ -f "$file" ]; then
            size=$(du -h "$file" | cut -f1)
            echo "  $file ($size)"
        fi
    done
else
    echo "❌ Hybrid collection failed with exit code $exit_code"
fi

exit $exit_code
