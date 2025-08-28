#!/bin/bash
#SBATCH --job-name=fix_standardization
#SBATCH --output=logs/fix_standardization_%j.out
#SBATCH --error=logs/fix_standardization_%j.err
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=ceab

echo "=== FIXING VARIABLE STANDARDIZATION ==="
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

echo "Running corrected standardization..."
Rscript scripts/r/standardize_variable_names_corrected.R

exit_code=$?

echo "=== Job Completion ==="
echo "Exit code: $exit_code"
echo "Completed at: $(date)"

if [ $exit_code -eq 0 ]; then
    echo "✅ Variable standardization fixed successfully"
else
    echo "❌ Variable standardization fix failed with exit code $exit_code"
fi

exit $exit_code
