#!/usr/bin/env bash
set -euo pipefail

# Run the full weather data pipeline without Slurm. Designed for standalone hosts
# such as cloud VMs or local machines where R and required system libraries are
# installed beforehand.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${REPO_ROOT}"

mkdir -p logs

BARCELONA_SENTINEL="data/output/barcelona_last_success.txt"
if [ -f "${BARCELONA_SENTINEL}" ]; then
    echo "Barcelona sentinel timestamp: $(cat "${BARCELONA_SENTINEL}")"
else
    echo "Barcelona sentinel not found; ensure update_barcelona_only.sh has run recently."
fi

echo "=== Collecting Three Datasets (Local) ==="
echo "Starting: $(date --utc) UTC"

run_step() {
    local description=$1
    shift
    echo "${description}..."
    set +e
    "$@"
    local status=$?
    set -e
    if [ ${status} -eq 0 ]; then
        echo "✅ ${description} completed"
    else
        echo "❌ ${description} failed"
    fi
    return ${status}
}

if ! run_step "Dataset 1: Historical daily stations" Rscript scripts/r/get_historical_data.R; then
    echo "Stopping after failure in Dataset 1"
    exit 1
fi

if ! run_step "Dataset 2: Current daily stations" Rscript scripts/r/aggregate_daily_stations_current.R; then
    echo "Stopping after failure in Dataset 2"
    exit 1
fi

if ! run_step "Dataset 3: Hourly station ongoing" Rscript scripts/r/get_latest_data.R; then
    echo "Stopping after failure in Dataset 3"
    exit 1
fi

KEY_POOLS=(a b c d e)
SHARD_COUNT=${#KEY_POOLS[@]}

echo "Dataset 4: Municipal forecasts (launching ${SHARD_COUNT} shards in parallel)..."

launch_forecast_shards() {
    local status=0
    local shard=0
    declare -a pids

    for key_pool in "${KEY_POOLS[@]}"; do
        shard=$((shard + 1))
        echo "  • Starting shard ${shard}/${SHARD_COUNT} with key pool '${key_pool}'"
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

if ! launch_forecast_shards; then
    echo "Municipal forecast job experienced failures"
    exit 1
fi

echo "=== Collection Summary ==="
echo "Completed: $(date --utc) UTC"
ls -la data/output/*.csv.gz 2>/dev/null || true
