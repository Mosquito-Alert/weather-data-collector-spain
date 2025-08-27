#!/bin/bash
#SBATCH --job-name=weather_original_names
#SBATCH --output=logs/weather_original_%j.out
#SBATCH --error=logs/weather_original_%j.err
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --partition=ceab

# WEATHER DATA COLLECTION - ORIGINAL NAMES APPROACH
# Collects 4 separate datasets with original AEMET variable names:
# 1. daily_stations_historical.csv.gz - Historical daily from climatological API
# 2. daily_stations_current.csv.gz - Recent daily from hourly aggregation
# 3. hourly_station_ongoing.csv.gz - Current hourly observations
# 4. daily_municipal_forecast.csv.gz - Municipal forecasts (validation collection)

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

# Run the original names collection approach
echo "Starting weather data collection with original AEMET variable names..."
echo "Strategy: 4 separate datasets, no variable renaming, data integrity preserved"
echo "Expected completion time: 2-4 hours"
echo ""

# Execute the original names collection script
Rscript scripts/r/collect_all_datasets_original_names.R

exit_code=$?

echo ""
echo "=== Job Completion ==="
echo "Exit code: $exit_code"
echo "Completed at: $(date)"

if [ $exit_code -eq 0 ]; then
    echo "✅ Original names collection completed successfully"
    
    # Show output file sizes
    echo ""
    echo "Generated files with original AEMET variable names:"
    for file in data/output/daily_stations_historical.csv.gz \
                data/output/daily_stations_current.csv.gz \
                data/output/hourly_station_ongoing.csv.gz \
                data/output/daily_municipal_forecast.csv.gz; do
        if [ -f "$file" ]; then
            size=$(du -h "$file" | cut -f1)
            records=$(gunzip -c "$file" 2>/dev/null | wc -l 2>/dev/null || echo "unknown")
            echo "  $file ($size, ~$records records)"
        else
            echo "  $file (missing)"
        fi
    done
    
    echo ""
    echo "Data integrity: All original AEMET variable names preserved"
    echo "Documentation: See docs/variable_names_reference.md for variable explanations"
else
    echo "❌ Collection failed with exit code $exit_code"
fi

exit $exit_code
