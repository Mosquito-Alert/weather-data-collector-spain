#!/usr/bin/env bash
set -euo pipefail

# Run only the municipal forecast collection without Slurm.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${REPO_ROOT}"

mkdir -p logs

KEY_POOLS=(a b c d e)
SHARD_COUNT=${#KEY_POOLS[@]}

launch_forecast_shards() {
    local status=0
    local shard=0
    declare -a pids

    for key_pool in "${KEY_POOLS[@]}"; do
        shard=$((shard + 1))
        echo "Launching shard ${shard}/${SHARD_COUNT} with key pool '${key_pool}'"
        (
            set -euo pipefail
            Rscript scripts/r/get_forecast_data_hybrid.R \
                --shard-index=${shard} \
                --shard-count=${SHARD_COUNT} \
                --key-pool=${key_pool}
        ) &
        pids[${shard}]=$!
    done

    for shard in "${!pids[@]}"; do
        if wait "${pids[${shard}]}"; then
            echo "✅ Forecast shard ${shard}/${SHARD_COUNT} completed"
        else
            echo "❌ Forecast shard ${shard}/${SHARD_COUNT} failed"
            status=1
        fi
    done

    return ${status}
}

if launch_forecast_shards; then
    echo "✅ Municipal forecasts completed"
else
    echo "❌ Municipal forecasts encountered failures"
    exit 1
fi

echo "Completed: $(date --utc) UTC"
ls -la data/output/municipal_forecasts*.csv.gz 2>/dev/null || true
