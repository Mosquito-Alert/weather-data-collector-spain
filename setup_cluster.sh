#!/bin/bash
# filepath: /Users/palmer/research/weather-data-collector-spain/setup_cluster.sh

# Load required modules
module load LibTIFF/4.6.0-GCCcore-13.3.0

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

# Setup script for first-time cluster deployment

echo "Setting up weather data collector on cluster..."

# Create necessary directories
mkdir -p logs
mkdir -p data/output
mkdir -p data/input

# Set permissions on auth directory (assuming you've manually copied keys)
chmod 700 auth
chmod 600 auth/keys.R

# Load R module
module load R/4.4.2-gfbf-2024a

# Initialize renv and install packages
echo "Installing R packages..."
R --slave --no-restore --file=- <<EOF
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos="https://cran.r-project.org")
}
renv::init()
renv::install(c(
  "tidyverse", 
  "lubridate", 
  "data.table", 
  "curl", 
  "jsonlite", 
  "httr", 
  "R.utils"
))
renv::snapshot()
EOF

echo "Setup complete!"
echo "You can now run:"
echo "  sbatch priority_municipal_data.sh     # For immediate model data"
echo "  sbatch update_weather.sh              # For complete data collection"
echo "  sbatch update_historical_weather.sh   # For historical data only"

# Run: bash setup_cluster.sh