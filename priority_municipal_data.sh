#!/bin/bash
#SBATCH --job-name=municipal-priority
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/municipal_priority_%j.out
#SBATCH --error=logs/municipal_priority_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/municipal_priority_%j.out
#SBATCH --error=logs/municipal_priority_%j.err

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0
module load Miniconda3/24.7.1-0

# Activate conda environment
conda activate mosquito-alert-monitor

# Load SSH agent since this is no longer done by default on the cluster
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

# Set locale environment variables
export LC_CTYPE=C.UTF-8
export LC_COLLATE=C.UTF-8
export LC_TIME=C.UTF-8
export LC_MESSAGES=C.UTF-8
export LC_MONETARY=C.UTF-8
export LC_PAPER=C.UTF-8
export LC_MEASUREMENT=C.UTF-8
export LANG=C.UTF-8

# Set working directory
cd ~/research/weather-data-collector-spain

# Create logs directory
mkdir -p logs

# Create output directory
mkdir -p data/output

# Activate renv
R --slave --no-restore --file=- <<EOF
renv::activate()
EOF

echo "Starting priority municipal data generation: $(date)"

# Get forecasts first (immediate availability)
echo "Collecting municipal forecasts for immediate model use..."
R CMD BATCH --no-save --no-restore code/get_forecast_data.R logs/priority_forecast_$(date +%Y%m%d_%H%M%S).out

# Generate backwards municipal data
echo "Generating municipal data backwards from present..."
R CMD BATCH --no-save --no-restore code/generate_municipal_priority.R logs/priority_municipal_$(date +%Y%m%d_%H%M%S).out

echo "Priority municipal data generation completed: $(date)"
echo "Models can now use: data/output/daily_municipal_extended.csv.gz"

# Submit: sbatch priority_municipal_data.sh