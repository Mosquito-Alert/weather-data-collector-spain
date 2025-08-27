#!/bin/bash
#SBATCH --job-name=weather_four_datasets
#SBATCH --output=logs/weather_four_datasets_%j.out
#SBATCH --error=logs/weather_four_datasets_%j.err
#SBATCH --time=06:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --partition=ceab

# FOUR-DATASET WEATHER COLLECTION SLURM JOB
# New approach: Separate datasets with original variable names
# Maintains data integrity by not mixing different API sources

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

# Run the four-dataset collection
echo "Starting four-dataset weather collection..."
echo "Approach: Original variable names, separate data sources"
echo "Expected completion time: 2-6 hours depending on data volumes"
echo ""

# Execute the four-dataset collection script
Rscript scripts/r/collect_four_datasets.R

exit_code=$?

echo ""
echo "=== Job Completion ==="
echo "Exit code: $exit_code"
echo "Completed at: $(date)"

if [ $exit_code -eq 0 ]; then
    echo "✅ Four-dataset collection completed successfully"
    
    # Show output file sizes
    echo ""
    echo "Generated datasets:"
    for file in data/output/daily_stations_historical.csv.gz \
                data/output/daily_stations_current.csv.gz \
                data/output/hourly_station_ongoing.csv.gz \
                data/output/daily_municipal_forecast.csv.gz; do
        if [ -f "$file" ]; then
            size=$(du -h "$file" | cut -f1)
            echo "  ✅ $file ($size)"
        else
            echo "  ❌ $file (missing)"
        fi
    done
    
    # Run quick validation
    echo ""
    echo "Running quick validation..."
    Rscript -e "
    files <- c('data/output/daily_stations_historical.csv.gz',
               'data/output/daily_stations_current.csv.gz', 
               'data/output/hourly_station_ongoing.csv.gz',
               'data/output/daily_municipal_forecast.csv.gz')
    for (f in files) {
      if (file.exists(f)) {
        d <- data.table::fread(f, nrows=5)
        cat('✅', basename(f), ': ', nrow(d), 'sample rows, cols:', paste(names(d)[1:min(5,ncol(d))], collapse=', '), '...\\n')
      } else {
        cat('❌', basename(f), ': missing\\n')
      }
    }"
    
else
    echo "❌ Four-dataset collection failed with exit code $exit_code"
    echo "Check the log file for details"
fi

exit $exit_code
