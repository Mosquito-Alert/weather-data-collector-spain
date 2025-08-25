#!/bin/bash
#SBATCH --job-name=test-modules
#SBATCH --partition=ceab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:05:00
#SBATCH --output=logs/test_modules_%j.out
#SBATCH --error=logs/test_modules_%j.err

# Load required modules (using the same as working scripts)
module load GDAL
module load R-bundle-CRAN
module load LibTIFF/4.6.0-GCCcore-13.3.0
module load cURL/8.7.1-GCCcore-13.3.0
module load OpenSSL/3

echo "Testing module loading fix..."
echo "R version:"
R --version | head -1
echo "R location:"
which R

echo "Testing if aggregation script can find R:"
cd ~/research/weather-data-collector-spain
if command -v R &> /dev/null; then
    echo "R is available in main script"
else
    echo "R is NOT available in main script"
fi

echo "Testing generate_all_datasets.sh R detection:"
./generate_all_datasets.sh --test-only 2>&1 | head -10
