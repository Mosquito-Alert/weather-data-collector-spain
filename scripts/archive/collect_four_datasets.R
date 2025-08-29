#!/usr/bin/env Rscript

# collect_four_datasets.R
# New approach: Collect 4 separate datasets with original variable names
# Maintains data integrity by not mixing different API sources

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
})

cat("========================================\n")
cat("FOUR-DATASET WEATHER COLLECTION\n") 
cat("========================================\n")
cat("Approach: Separate datasets, original variable names\n")
cat("Started at:", format(Sys.time()), "\n\n")

# Configuration
COLLECT_HISTORICAL = TRUE
COLLECT_CURRENT = TRUE  
COLLECT_HOURLY = TRUE
COLLECT_FORECAST = TRUE

start_time <- Sys.time()
results <- list()

# === DATASET 1: HISTORICAL DAILY STATIONS ===
if (COLLECT_HISTORICAL) {
  cat("=== DATASET 1: HISTORICAL DAILY STATIONS ===\n")
  cat("Source: AEMET historical climatological API (2013 to T-4 days)\n")
  cat("Output: daily_stations_historical.csv.gz\n")
  
  dataset1_start <- Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_stations_historical.R")
    dataset1_end <- Sys.time()
    results$historical <- list(
      success = TRUE,
      duration = as.numeric(difftime(dataset1_end, dataset1_start, units = "mins"))
    )
    cat("✅ Dataset 1 completed in", round(results$historical$duration, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 1 failed:", e$message, "\n\n")
    results$historical <- list(success = FALSE, error = e$message)
  })
}

# === DATASET 2: CURRENT DAILY STATIONS ===
if (COLLECT_CURRENT) {
  cat("=== DATASET 2: CURRENT DAILY STATIONS ===\n")
  cat("Source: Hourly API aggregated to daily (T-4 days to yesterday)\n")
  cat("Output: daily_stations_current.csv.gz\n")
  
  dataset2_start <- Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_stations_current.R")
    dataset2_end <- Sys.time()
    results$current <- list(
      success = TRUE,
      duration = as.numeric(difftime(dataset2_end, dataset2_start, units = "mins"))
    )
    cat("✅ Dataset 2 completed in", round(results$current$duration, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 2 failed:", e$message, "\n\n")
    results$current <- list(success = FALSE, error = e$message)
  })
}

# === DATASET 3: HOURLY STATIONS ===
if (COLLECT_HOURLY) {
  cat("=== DATASET 3: HOURLY STATIONS ===\n")
  cat("Source: AEMET hourly observations API\n")
  cat("Output: hourly_station_ongoing.csv.gz\n")
  
  dataset3_start <- Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_hourly_station_ongoing.R")
    dataset3_end <- Sys.time()
    results$hourly <- list(
      success = TRUE,
      duration = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    )
    cat("✅ Dataset 3 completed in", round(results$hourly$duration, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    results$hourly <- list(success = FALSE, error = e$message)
  })
}

# === DATASET 4: MUNICIPAL FORECASTS ===
if (COLLECT_FORECAST) {
  cat("=== DATASET 4: MUNICIPAL FORECASTS ===\n")
  cat("Source: AEMET municipal forecast API\n")
  cat("Output: daily_municipal_forecast.csv.gz\n")
  
  dataset4_start <- Sys.time()
  
  tryCatch({
    source("scripts/r/aggregate_daily_municipal_forecast.R")
    dataset4_end <- Sys.time()
    results$forecast <- list(
      success = TRUE,
      duration = as.numeric(difftime(dataset4_end, dataset4_start, units = "mins"))
    )
    cat("✅ Dataset 4 completed in", round(results$forecast$duration, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 4 failed:", e$message, "\n\n")
    results$forecast <- list(success = FALSE, error = e$message)
  })
}

# === FINAL SUMMARY ===
end_time <- Sys.time()
total_time <- as.numeric(difftime(end_time, start_time, units = "mins"))

cat("========================================\n")
cat("FOUR-DATASET COLLECTION SUMMARY\n")
cat("========================================\n")
cat("Total execution time:", round(total_time, 2), "minutes\n\n")

# Individual dataset results
for (dataset_name in names(results)) {
  result <- results[[dataset_name]]
  if (result$success) {
    cat("✅", toupper(dataset_name), "dataset:", round(result$duration, 2), "minutes\n")
  } else {
    cat("❌", toupper(dataset_name), "dataset: FAILED -", result$error, "\n")
  }
}

cat("\nOutput files status:\n")
expected_files <- c(
  "daily_stations_historical.csv.gz",
  "daily_stations_current.csv.gz", 
  "hourly_station_ongoing.csv.gz",
  "daily_municipal_forecast.csv.gz"
)

for (file in expected_files) {
  filepath <- file.path("data/output", file)
  if (file.exists(filepath)) {
    file_size <- round(file.size(filepath) / 1024 / 1024, 2)
    
    # Get basic file info
    tryCatch({
      dt <- fread(filepath, nrows = 0)  # Just get column info
      cols <- ncol(dt)
      
      # Get row count more efficiently
      dt_sample <- fread(filepath, nrows = 1000)
      if (nrow(dt_sample) < 1000) {
        rows <- nrow(dt_sample)
      } else {
        # Estimate rows from file size for large files
        sample_size <- object.size(dt_sample)
        total_size <- file.size(filepath)
        rows <- round(as.numeric(total_size / sample_size * nrow(dt_sample)))
      }
      
      cat("  ✅", file, "(", file_size, "MB,", rows, "rows,", cols, "cols)\n")
    }, error = function(e) {
      cat("  ✅", file, "(", file_size, "MB, format check failed)\n")
    })
  } else {
    cat("  ❌", file, "(not found)\n")
  }
}

cat("\nCompleted at:", format(Sys.time()), "\n")
cat("========================================\n")

# Summary of approach
cat("\nFOUR-DATASET APPROACH SUMMARY:\n")
cat("1. daily_stations_historical.csv.gz - Historical climatological data (2013 to T-4)\n")
cat("2. daily_stations_current.csv.gz - Recent daily data from hourly aggregation (T-4 to yesterday)\n") 
cat("3. hourly_station_ongoing.csv.gz - Current hourly observations\n")
cat("4. daily_municipal_forecast.csv.gz - Municipal forecasts for validation\n")
cat("\nAll datasets preserve original AEMET variable names for data integrity.\n")
