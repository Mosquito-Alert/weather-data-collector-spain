#!/bin/bash
#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=update_weather
#SBATCH --output=logs/update_weather_%j.out
#SBATCH --error=logs/update_weather_%j.err

# Initialize Lmod with proper MODULEPATH
source /opt/ohpc/admin/lmod/lmod/init/bash
export MODULEPATH=/opt/ohpc/pub/modulefiles:/software/eb/modules/all:/software/eb/modules/toolchain

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

BARCELONA_SENTINEL="data/output/barcelona_last_success.txt"
if [ -f "${BARCELONA_SENTINEL}" ]; then
    echo "Barcelona sentinel timestamp: $(cat "${BARCELONA_SENTINEL}")"
else
    echo "Barcelona sentinel not found; ensure update_barcelona_only.sh has run recently."
fi

echo "=== Collecting Three Datasets ==="
echo "Starting: $(date)"

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
srun Rscript scripts/r/aggregate_daily_stations_current.R
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

# Dataset 4: Municipal forecasts (run as a standalone SLURM job)
echo "Dataset 4: Municipal forecasts..."
FORECAST_JOB_ID=$(sbatch --parsable update_municipal_forecasts_only.sh)
if [ $? -eq 0 ]; then
    echo "✅ Forecast collection submitted as job ${FORECAST_JOB_ID}"
else
    echo "❌ Forecast collection submission failed"
fi

echo "=== Collection Summary ==="
echo "Completed: $(date)"
ls -la data/output/*.csv.gz

# Run with
# sbatch ~/research/weather-data-collector-spain/update_weather.sh