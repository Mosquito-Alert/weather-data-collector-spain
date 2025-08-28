#!/bin/bash
# filepath: scripts/update_weather.sh

#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=update_weather

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

cat "=== Collecting Three Datasets ==="
cat "Starting: $(date)"

# Dataset 1: Historical daily stations (original AEMET names)
echo "Dataset 1: Historical daily stations..."
srun Rscript scripts/r/get_historical_data.R
if [ $? -eq 0 ]; then
    echo "✅ Historical collection completed"
else
    echo "❌ Historical collection failed"
fi

# Dataset 2: Current daily stations (gap between historical and present)
echo "Dataset 2: Current daily stations..."
srun Rscript scripts/r/aggregate_current_daily_stations.R
if [ $? -eq 0 ]; then
    echo "✅ Current daily collection completed"
else
    echo "❌ Current daily collection failed"
fi

# Dataset 3: Hourly station ongoing
echo "Dataset 3: Hourly station ongoing..."
srun Rscript scripts/r/get_latest_data.R
if [ $? -eq 0 ]; then
    echo "✅ Hourly collection completed"
else
    echo "❌ Hourly collection failed"
fi

# Dataset 4: Municipal forecasts
echo "Dataset 4: Municipal forecasts..."
srun Rscript scripts/r/get_forecast_data_hybrid.R
if [ $? -eq 0 ]; then
    echo "✅ Forecast collection completed"
else
    echo "❌ Forecast collection failed"
fi

echo "=== Collection Summary ==="
echo "Completed: $(date)"
ls -la data/output/*.csv.gz