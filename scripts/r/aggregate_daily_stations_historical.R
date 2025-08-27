#!/usr/bin/env Rscript

# aggregate_daily_stations_historical.R
# Produces daily_stations_historical.csv.gz from AEMET historical climatological data
# Keeps original variable names for data integrity

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
})

cat("=== Daily Stations Historical Aggregation ===\n")
cat("Source: AEMET historical climatological API\n")
cat("Period: 2013 to T-4 days (as far as historical API provides)\n")
cat("Variables: Original AEMET names preserved\n\n")

# Check if we have historical data collection
historical_files <- list.files("data/output", pattern = "historical.*\\.csv(\\.gz)?$", full.names = TRUE)
if (length(historical_files) == 0) {
  cat("No historical data files found. Running historical data collection...\n")
  tryCatch({
    source("scripts/r/get_historical_data.R")
  }, error = function(e) {
    cat("❌ Historical data collection failed:", e$message, "\n")
    stop("Cannot proceed without historical data collection")
  })
  
  # Re-check for historical files
  historical_files <- list.files("data/output", pattern = "historical.*\\.csv(\\.gz)?$", full.names = TRUE)
}

if (length(historical_files) == 0) {
  stop("No historical data available. Please run historical data collection first.")
}

# Use the most recent historical file
historical_file <- historical_files[which.max(file.mtime(historical_files))]
cat("Using historical file:", basename(historical_file), "\n")

# Load historical data
historical_data <- fread(historical_file)
cat("Loaded", nrow(historical_data), "historical records\n")
cat("Date range:", min(historical_data$fecha, na.rm = TRUE), "to", max(historical_data$fecha, na.rm = TRUE), "\n")
cat("Stations:", length(unique(historical_data$indicativo)), "\n")
cat("Variables:", paste(names(historical_data), collapse = ", "), "\n")

# Basic data validation
if (!"fecha" %in% names(historical_data)) {
  stop("Historical data missing 'fecha' column")
}
if (!"indicativo" %in% names(historical_data)) {
  stop("Historical data missing 'indicativo' (station ID) column")
}

# Ensure fecha is Date type
historical_data$fecha <- as.Date(historical_data$fecha)

# Sort by date and station
historical_data <- historical_data[order(fecha, indicativo)]

# Add collection metadata
historical_data$collection_timestamp <- Sys.time()
historical_data$data_source <- "aemet_historical_api"

# Save as daily_stations_historical.csv.gz
output_file <- "data/output/daily_stations_historical.csv.gz"
fwrite(historical_data, output_file)

cat("\n=== Historical Aggregation Complete ===\n")
cat("Output file:", output_file, "\n")
cat("Records:", nrow(historical_data), "\n")
cat("File size:", round(file.size(output_file)/1024/1024, 1), "MB\n")
cat("Date range:", min(historical_data$fecha, na.rm = TRUE), "to", max(historical_data$fecha, na.rm = TRUE), "\n")
cat("Stations:", length(unique(historical_data$indicativo)), "\n")

# Summary by variable (check data completeness)
cat("\nData completeness by variable:\n")
numeric_vars <- names(historical_data)[sapply(historical_data, is.numeric)]
for (var in numeric_vars) {
  if (var != "indicativo") {  # Skip station ID
    completeness <- round(100 * sum(!is.na(historical_data[[var]])) / nrow(historical_data), 1)
    cat(sprintf("  %s: %s%% complete\n", var, completeness))
  }
}
