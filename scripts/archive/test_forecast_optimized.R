#!/usr/bin/env Rscript

# Quick test of the optimized forecast collection
source("code/get_forecast_data_optimized.R")

# Override settings for quick test
TESTING_MODE = TRUE
N_TEST_MUNICIPALITIES = 5

cat("Testing optimized forecast collection with", N_TEST_MUNICIPALITIES, "municipalities...\n")
