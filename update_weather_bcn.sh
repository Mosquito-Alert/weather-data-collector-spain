#!/bin/bash
#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=update_weather_bcn
#SBATCH --output=logs/update_weather_bcn_%j.out
#SBATCH --error=logs/update_weather_bcn_%j.err

# Initialize Lmod
source /opt/ohpc/admin/lmod/lmod/init/bash
export MODULEPATH=/opt/ohpc/pub/modulefiles:/software/eb/modules/all:/software/eb/modules/toolchain

module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

cd /home/j.palmer/research/weather-data-collector-spain || exit 1

echo "=== Collecting Barcelona-Only Weather Datasets ==="
echo "Starting: $(date)"

PIPELINE_FAILED=0

run_step() {
    local label="$1"
    local script_path="$2"

    echo "${label}..."
    if srun Rscript "${script_path}"; then
        echo "✅ ${label} completed"
    else
        echo "❌ ${label} failed"
        PIPELINE_FAILED=1
    fi
    echo
}

run_step "Dataset 1: Historical daily stations (Barcelona)" scripts/r/get_historical_data_barcelona.R
run_step "Dataset 2: Hourly station ongoing (Barcelona)" scripts/r/get_latest_data_barcelona.R
run_step "Dataset 3: Current daily stations (Barcelona)" scripts/r/aggregate_daily_stations_current_barcelona.R
run_step "Dataset 4: Municipal forecast (Barcelona)" scripts/r/get_forecast_barcelona.R

echo "=== Barcelona Collection Summary ==="
echo "Completed: $(date)"
ls -la data/output/*barcelona*.csv.gz 2>/dev/null

echo "Run with"
echo "sbatch ~/research/weather-data-collector-spain/update_weather_bcn.sh"

if [ ${PIPELINE_FAILED} -ne 0 ]; then
    echo "One or more Barcelona dataset steps failed." >&2
    exit 1
fi
