#!/bin/bash
#SBATCH --partition=ceab
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:30:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=johnrbpalmer@gmail.com
#SBATCH --job-name=hist_bcn
#SBATCH --output=logs/hist_bcn_%j.out
#SBATCH --error=logs/hist_bcn_%j.err

source /opt/ohpc/admin/lmod/lmod/init/bash
export MODULEPATH=/opt/ohpc/pub/modulefiles:/software/eb/modules/all:/software/eb/modules/toolchain

module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

cd /home/j.palmer/research/weather-data-collector-spain || exit 1

srun Rscript scripts/r/get_historical_data_barcelona.R
