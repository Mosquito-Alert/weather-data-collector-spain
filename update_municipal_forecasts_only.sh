#!/bin/bash
#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --array=1-5%5
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=update_municipal_forecasts
#SBATCH --output=logs/update_municipal_forecasts_%A_%a.out
#SBATCH --error=logs/update_municipal_forecasts_%A_%a.err

# Initialize Lmod with proper MODULEPATH
source /opt/ohpc/admin/lmod/lmod/init/bash
export MODULEPATH=/opt/ohpc/pub/modulefiles:/software/eb/modules/all:/software/eb/modules/toolchain

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

# Dataset 4: Municipal forecasts
# Define shard-to-key mapping (one shard per API key)
KEY_POOLS=("a" "b" "c" "d" "e")
SHARD_COUNT=${#KEY_POOLS[@]}
SHARD_INDEX=${SLURM_ARRAY_TASK_ID:-1}

if [ ${SHARD_INDEX} -lt 1 ] || [ ${SHARD_INDEX} -gt ${SHARD_COUNT} ]; then
    echo "Shard index ${SHARD_INDEX} is out of bounds for ${SHARD_COUNT} configured shards. Exiting."
    exit 1
fi

KEY_POOL=${KEY_POOLS[$((SHARD_INDEX-1))]}

# Dataset 4: Municipal forecasts
EXIT_CODE=0
echo "Dataset 4: Municipal forecasts shard ${SHARD_INDEX}/${SHARD_COUNT} using key pool '${KEY_POOL}'..."
srun Rscript scripts/r/get_forecast_data_hybrid.R \
    --shard-index=${SHARD_INDEX} \
    --shard-count=${SHARD_COUNT} \
    --key-pool=${KEY_POOL}
if [ $? -eq 0 ]; then
    echo "✅ Forecast collection completed"
else
    echo "❌ Forecast collection failed"
    EXIT_CODE=1
fi

echo "=== Collection Summary ==="
echo "Completed: $(date)"
ls -la data/output/*.csv.gz

if [ "${SLURM_ARRAY_TASK_ID:-1}" -eq 1 ]; then
    echo "Running municipal forecast coverage audit..."
    if python3 scripts/python/audit_municipal_forecast_coverage.py; then
        echo "✅ Municipal forecast coverage audit passed"
    else
        echo "❌ Municipal forecast coverage audit failed"
        EXIT_CODE=1
    fi
fi

exit ${EXIT_CODE}

# Run with
# sbatch ~/research/weather-data-collector-spain/update_municipal_forecasts_only.sh