#!/usr/bin/env Rscript

# collect_all_datasets_original_names.R
# Main orchestrator for the 4-dataset approach with original variable names
# Produces: daily_stations_historical.csv.gz, daily_stations_current.csv.gz,
#           hourly_station_ongoing.csv.gz, daily_municipal_forecast.csv.gz

rm(list=ls())

library(tidyverse)
library(lubridate)
library(data.table)

cat("=======================================\n")
cat("WEATHER DATA COLLECTION - ORIGINAL NAMES\n") 
cat("=======================================\n")
cat("Started at:", format(Sys.time()), "\n\n")

cat("Strategy: 4 separate datasets with original AEMET variable names\n")
cat("1. daily_stations_historical.csv.gz - Historical API (2013 to T-4 days)\n")
cat("2. daily_stations_current.csv.gz - Hourly aggregated (gap between historical and present)\n") 
cat("3. hourly_station_ongoing.csv.gz - Current hourly observations\n")
cat("4. daily_municipal_forecast.csv.gz - Municipal forecasts (ongoing collection)\n\n")

# Configuration
COLLECT_HISTORICAL = TRUE
COLLECT_CURRENT = TRUE  
COLLECT_HOURLY = TRUE
COLLECT_FORECASTS = TRUE

start_time = Sys.time()
times = list()

# === DATASET 1: HISTORICAL DAILY STATIONS ===
if(COLLECT_HISTORICAL) {
  cat("=== DATASET 1: HISTORICAL DAILY STATIONS ===\n")
  cat("Source: AEMET historical climatological API\n")
  
  dataset1_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_stations_historical.R")
    dataset1_end = Sys.time()
    times$historical = as.numeric(difftime(dataset1_end, dataset1_start, units = "mins"))
    cat("✅ Dataset 1 completed in", round(times$historical, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 1 failed:", e$message, "\n\n")
    times$historical = NA
  })
}

# === DATASET 2: CURRENT DAILY STATIONS ===
if(COLLECT_CURRENT) {
  cat("=== DATASET 2: CURRENT DAILY STATIONS ===\n")
  cat("Source: Hourly data aggregated to daily (gap period)\n")
  
  dataset2_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_stations_current.R")
    dataset2_end = Sys.time()
    times$current = as.numeric(difftime(dataset2_end, dataset2_start, units = "mins"))
    cat("✅ Dataset 2 completed in", round(times$current, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 2 failed:", e$message, "\n\n")
    times$current = NA
  })
}

# === DATASET 3: HOURLY ONGOING ===
if(COLLECT_HOURLY) {
  cat("=== DATASET 3: HOURLY ONGOING ===\n")
  cat("Source: AEMET current hourly API\n")
  
  dataset3_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_hourly_station_ongoing.R")
    dataset3_end = Sys.time()
    times$hourly = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    cat("✅ Dataset 3 completed in", round(times$hourly, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    times$hourly = NA
  })
}

# === DATASET 4: MUNICIPAL FORECASTS ===
if(COLLECT_FORECASTS) {
  cat("=== DATASET 4: MUNICIPAL FORECASTS ===\n")
  cat("Source: AEMET municipal forecast API\n")
  
  dataset4_start = Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_municipal_forecast.R")
    dataset4_end = Sys.time()
    times$forecasts = as.numeric(difftime(dataset4_end, dataset4_start, units = "mins"))
    cat("✅ Dataset 4 completed in", round(times$forecasts, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 4 failed:", e$message, "\n\n")
    times$forecasts = NA
  })
}

# === FINAL SUMMARY ===
end_time = Sys.time()
total_time = as.numeric(difftime(end_time, start_time, units = "mins"))

cat("\n========================================\n")
cat("COLLECTION SUMMARY\n")
cat("========================================\n")
cat("Total execution time:", round(total_time, 2), "minutes\n\n")

cat("Individual dataset times:\n")
if(!is.na(times$historical)) cat("  Historical Daily:", round(times$historical, 2), "minutes\n")
if(!is.na(times$current)) cat("  Current Daily:", round(times$current, 2), "minutes\n")
if(!is.na(times$hourly)) cat("  Hourly Ongoing:", round(times$hourly, 2), "minutes\n")
if(!is.na(times$forecasts)) cat("  Municipal Forecasts:", round(times$forecasts, 2), "minutes\n")

cat("\nOutput files with original variable names:\n")
files_to_check = c(
  "data/output/daily_stations_historical.csv.gz",
  "data/output/daily_stations_current.csv.gz", 
  "data/output/hourly_station_ongoing.csv.gz",
  "data/output/daily_municipal_forecast.csv.gz"
)

for(file in files_to_check) {
  if(file.exists(file)) {
    file_size = round(file.size(file) / 1024 / 1024, 2)
    file_info = file.info(file)
    cat("  ✅", basename(file), "(", file_size, "MB, modified:", format(file_info$mtime, "%Y-%m-%d %H:%M"), ")\n")
  } else {
    cat("  ❌", basename(file), "(not found)\n")
  }
}

# Quick data validation
cat("\nData validation:\n")
for(file in files_to_check) {
  if(file.exists(file)) {
    tryCatch({
      sample_data = fread(file, nrows = 5)
      cat("  ", basename(file), "- Columns:", paste(names(sample_data)[1:min(5, ncol(sample_data))], collapse = ", "), 
          ifelse(ncol(sample_data) > 5, "...", ""), "\n")
    }, error = function(e) {
      cat("  ", basename(file), "- Error reading:", e$message, "\n")
    })
  }
}

cat("\nCompleted at:", format(Sys.time()), "\n")
cat("Data integrity: Preserved with original AEMET variable names\n")
