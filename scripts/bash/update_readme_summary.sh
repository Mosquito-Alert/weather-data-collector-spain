#!/bin/bash
#SBATCH --job-name=readme_update
#SBATCH --output=logs/readme_update_%j.out
#SBATCH --error=logs/readme_update_%j.err
#SBATCH --time=00:30:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --partition=ceab

echo "=== README Update Job ===
Job ID: $SLURM_JOB_ID
Node: $HOSTNAME
Started at: $(date)"

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

echo "Updating README with current data summary..."
Rscript scripts/r/update_readme_with_summary.R

echo "=== Job Completion ==="
echo "Exit code: $?"
echo "Completed at: $(date)"
