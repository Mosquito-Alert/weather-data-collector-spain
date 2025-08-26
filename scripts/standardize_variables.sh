#!/bin/bash
#SBATCH --job-name=standardize_vars
#SBATCH --output=logs/standardize_vars_%j.out
#SBATCH --error=logs/standardize_vars_%j.err
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=ceab

echo "=== Variable Standardization Job ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Started at: $(date)"

# Load required modules
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3

# Change to project directory
cd /home/j.palmer/research/weather-data-collector-spain

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Starting variable standardization..."
Rscript code/standardize_variable_names.R

exit_code=$?

echo "=== Job Completion ==="
echo "Exit code: $exit_code"
echo "Completed at: $(date)"

if [ $exit_code -eq 0 ]; then
    echo "✅ Variable standardization completed successfully"
else
    echo "❌ Variable standardization failed with exit code $exit_code"
fi

exit $exit_code
