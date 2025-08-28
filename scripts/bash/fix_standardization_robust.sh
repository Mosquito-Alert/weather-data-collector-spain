#!/bin/bash
#SBATCH --job-name=fix_standardization_robust
#SBATCH --output=/home/j.palmer/research/weather-data-collector-spain/logs/fix_standardization_robust_%j.out
#SBATCH --error=/home/j.palmer/research/weather-data-collector-spain/logs/fix_standardization_robust_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1

# Load required modules
module purge
module load R/4.4.2-gfbf-2024a

# Change to working directory
cd /home/j.palmer/research/weather-data-collector-spain

# Run the robust standardization script
echo "Starting robust variable standardization..."
Rscript scripts/r/standardize_variable_names_robust.R

echo "Standardization job completed."
