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

# Dataset 4: Municipal forecasts
echo "Dataset 4: Municipal forecasts..."
srun Rscript scripts/r/get_forecast_barcelona.R
if [ $? -eq 0 ]; then
    echo "✅ Forecast collection completed"
else
    echo "❌ Forecast collection failed"
fi

echo "=== Collection Summary ==="
echo "Completed: $(date)"
ls -la data/output/*.csv.gz

# Run with
# sbatch ~/research/weather-data-collector-spain/update_bcn_municipal_forecasts_only.sh