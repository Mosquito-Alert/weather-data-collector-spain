#!/usr/bin/env Rscript

# aggregate_municipal_daily.R
# --------------------------
# Purpose: Create Dataset 2 - Daily municipal data combining station observations with forecasts
# 
# This script:
# 1. Aggregates station observations to municipal level for historical/recent days
# 2. Appends municipal forecast data for future days
# 3. Produces a unified municipal daily dataset covering historical + forecast periods
#
# Output: data/output/daily_municipal_extended.csv.gz

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== Municipal Daily Data Aggregation ===\n")
cat("Started at:", format(Sys.time()), "\n")

# This script will be implemented to combine:
# - Daily station data aggregated to municipal level
# - Municipal forecast data
# - Creating the unified Dataset 2 as specified in specs.md

cat("Script structure ready - implementation needed for municipal aggregation\n")
cat("Will combine station observations + municipal forecasts\n")
