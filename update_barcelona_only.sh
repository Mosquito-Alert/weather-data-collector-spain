#!/bin/bash
#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=update_barcelona
#SBATCH --output=logs/update_barcelona_%j.out
#SBATCH --error=logs/update_barcelona_%j.err

# Initialize Lmod with proper MODULEPATH
source /opt/ohpc/admin/lmod/lmod/init/bash
export MODULEPATH=/opt/ohpc/pub/modulefiles:/software/eb/modules/all:/software/eb/modules/toolchain

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

echo "=== Collecting Barcelona Datasets ==="
echo "Starting: $(date --utc) UTC"

SENTINEL_FILE="data/output/barcelona_last_success.txt"
JOB_FAILED=0

run_step() {
    local description=$1
    shift
    echo "${description}..."
    srun "$@"
    local status=$?
    if [ ${status} -eq 0 ]; then
        echo "✅ ${description} completed"
    else
        echo "❌ ${description} failed"
        JOB_FAILED=1
    fi
    return ${status}
}

if ! run_step "Barcelona Dataset 1: Historical daily stations" Rscript scripts/r/get_historical_data_barcelona.R --key-pool=a,b,c,d,e; then
    echo "Stopping Barcelona job after failure in Dataset 1"
    exit 1
fi

if ! run_step "Barcelona Dataset 2: Current daily stations" Rscript scripts/r/aggregate_daily_stations_current_barcelona.R; then
    echo "Stopping Barcelona job after failure in Dataset 2"
    exit 1
fi

if ! run_step "Barcelona Dataset 3: Hourly station ongoing" Rscript scripts/r/get_latest_data_barcelona.R --key-pool=a,b,c,d,e; then
    echo "Stopping Barcelona job after failure in Dataset 3"
    exit 1
fi

if ! run_step "Barcelona Dataset 4: Municipal forecasts" Rscript scripts/r/get_forecast_barcelona.R --key-pool=a,b,c,d,e --elaborado-max-attempts=4 --elaborado-wait-seconds=900; then
    echo "Stopping Barcelona job after failure in Dataset 4"
    exit 1
fi

if [ ${JOB_FAILED} -eq 0 ]; then
    date --utc > "${SENTINEL_FILE}"
    echo "Barcelona sentinel updated at $(cat "${SENTINEL_FILE}")"
else
    echo "Barcelona job experienced failures; sentinel not updated"
fi

echo "=== Barcelona Collection Summary ==="
echo "Completed: $(date --utc) UTC"
ls -la data/output/*barcelona*.csv.gz 2>/dev/null

# Run with
# sbatch ~/research/weather-data-collector-spain/update_barcelona_only.sh
