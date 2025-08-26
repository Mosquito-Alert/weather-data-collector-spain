#!/bin/bash
#SBATCH --job-name=fill_gaps
#SBATCH --output=logs/fill_gaps_%j.out
#SBATCH --error=logs/fill_gaps_%j.err
#SBATCH --time=02:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=ceab

echo "=== Gap Filling Job ===
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

echo "Starting gap filling analysis..."
Rscript code/fill_data_gaps.R

echo "=== Job Completion ==="
echo "Exit code: $?"
echo "Completed at: $(date)"
