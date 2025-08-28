#!/usr/bin/env Rscript

# collect_all_datasets_standardized.R
# Updated version that produces standardized variable names
# Replaces collect_all_datasets_consolidated.R

rm(list=ls())

library(tidyverse)
library(lubridate)
library(data.table)

cat("=======================================\n")
cat("STANDARDIZED WEATHER DATA COLLECTION\n") 
cat("=======================================\n")
cat("Started at:", format(Sys.time()), "\n\n")

# Configuration
COLLECT_STATION_DATA = TRUE
COLLECT_MUNICIPAL_FORECASTS = TRUE  
COLLECT_HOURLY_DATA = TRUE

start_time = Sys.time()
times = list()

# === DATASET 1: DAILY STATION DATA ===
if(COLLECT_STATION_DATA) {
  cat("=== DATASET 1: DAILY STATION DATA ===\n")
  cat("Collecting daily means, minimums, and maximums by weather station\n")
  
  dataset1_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_station_standardized.R")
    dataset1_end = Sys.time()
    times$station_daily = as.numeric(difftime(dataset1_end, dataset1_start, units = "mins"))
    cat("✅ Dataset 1 completed in", round(times$station_daily, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 1 failed:", e$message, "\n\n")
    times$station_daily = NA
  })
}

# === DATASET 2: MUNICIPAL FORECASTS ===
if(COLLECT_MUNICIPAL_FORECASTS) {
  cat("=== DATASET 2: MUNICIPAL FORECASTS ===\n")
  cat("Collecting municipal data with 7-day forecasts using climaemet\n")
  
  dataset2_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_municipal_standardized.R")
    dataset2_end = Sys.time()
    times$municipal_forecasts = as.numeric(difftime(dataset2_end, dataset2_start, units = "mins"))
    cat("✅ Dataset 2 completed in", round(times$municipal_forecasts, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 2 failed:", e$message, "\n\n")
    times$municipal_forecasts = NA
  })
}

# === DATASET 3: HOURLY DATA ===
if(COLLECT_HOURLY_DATA) {
  cat("=== DATASET 3: HOURLY DATA ===\n")
  cat("Collecting hourly data for building history\n")
  
  dataset3_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_hourly_standardized.R")
    dataset3_end = Sys.time()
    times$hourly_data = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    cat("✅ Dataset 3 completed in", round(times$hourly_data, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    times$hourly_data = NA
  })
}

# === POST-COLLECTION AGGREGATION ===
cat("=== POST-COLLECTION AGGREGATION ===\n")
cat("(Aggregation already performed by standardized wrappers)\n")

# === FINAL GAP ANALYSIS ===
cat("\n=== POST-COLLECTION GAP ANALYSIS ===\n")
tryCatch({
  source("code/check_data_gaps.R")
  cat("✅ Gap analysis completed\n")
}, error = function(e) {
  cat("❌ Gap analysis failed:", e$message, "\n")
})

# === UPDATE README ===
cat("\n=== UPDATING README SUMMARY ===\n")
tryCatch({
  source("code/update_readme_with_summary.R")
  cat("✅ README update completed\n")
}, error = function(e) {
  cat("❌ README update failed:", e$message, "\n")
})

# === FINAL SUMMARY ===
end_time = Sys.time()
total_time = as.numeric(difftime(end_time, start_time, units = "mins"))

cat("\n========================================\n")
cat("COLLECTION SUMMARY\n")
cat("========================================\n")
cat("Total execution time:", round(total_time, 2), "minutes\n\n")

cat("Individual dataset times:\n")
if(exists("times") && !is.null(times$station_daily) && !is.na(times$station_daily)) {
  cat("  Dataset 1 (Station Daily):", round(times$station_daily, 2), "minutes\n")
}
if(exists("times") && !is.null(times$municipal_forecasts) && !is.na(times$municipal_forecasts)) {
  cat("  Dataset 2 (Municipal Forecasts):", round(times$municipal_forecasts, 2), "minutes\n")
}
if(exists("times") && !is.null(times$hourly_data) && !is.na(times$hourly_data)) {
  cat("  Dataset 3 (Hourly Data):", round(times$hourly_data, 2), "minutes\n")
}

cat("\nStandardized output files:\n")
files_to_check = c(
  "data/output/daily_station_historical.csv",
  "data/output/daily_municipal_extended.csv", 
  "data/output/hourly_station_ongoing.csv"
)

for(file in files_to_check) {
  if(file.exists(file)) {
    file_size = round(file.size(file) / 1024 / 1024, 2)
    rows = nrow(fread(file, nrows=0))
    cat("  ✅", basename(file), "(", file_size, "MB )\n")
  } else {
    cat("  ❌", basename(file), "(not found)\n")
  }
}

cat("\nCompleted at:", format(Sys.time()), "\n")
