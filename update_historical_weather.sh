#!/bin/bash
#SBATCH --job-name=historical-weather
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=6G
#SBATCH --time=04:00:00
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --output=logs/historical_weather_%j.out
#SBATCH --error=logs/historical_weather_%j.err

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load R/4.4.2-gfbf-2024a
module load cURL/8.7.1-GCCcore-13.3.0

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

# Pull any pending commits
git pull origin main

echo "Starting historical weather data update: $(date)"

# Update historical data
R CMD BATCH --no-save --no-restore code/get_historical_data.R logs/get_historical_data_$(date +%Y%m%d_%H%M%S).out

echo "Historical weather data update completed: $(date)"

# Commit and push the log files from this latest run
git add logs/*.out
git commit -m "Historical weather data update: $(date +%Y-%m-%d_%H:%M:%S) (cluster - automated)"
git pull origin main
git push origin main

# Submit: sbatch update_historical_weather.sh