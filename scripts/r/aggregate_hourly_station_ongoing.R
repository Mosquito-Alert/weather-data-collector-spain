#!/usr/bin/env Rscript

# aggregate_hourly_station_ongoing.R
# Produces hourly_station_ongoing.csv.gz from AEMET hourly API
# Keeps original variable names for data integrity
# This is ongoing collection of the most recent hourly observations

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
})

cat("=== Hourly Station Ongoing Collection ===\n")
cat("Source: AEMET hourly observations API\n")
cat("Purpose: Most recent hourly weather observations\n")
cat("Variables: Original AEMET names preserved\n\n")

# Check if hourly data already exists and is recent
hourly_files <- c(
  "data/output/hourly_station_ongoing.csv.gz",
  "data/output/hourly_station_ongoing.csv"
)

existing_file <- hourly_files[file.exists(hourly_files)][1]
needs_update <- TRUE

if (!is.na(existing_file)) {
  file_age_hours <- as.numeric(difftime(Sys.time(), file.mtime(existing_file), units = "hours"))
  cat("Existing hourly file found, age:", round(file_age_hours, 1), "hours\n")
  
  if (file_age_hours < 6) {  # If less than 6 hours old, may not need update
    cat("File is recent, checking if update needed...\n")
    existing_data <- fread(existing_file, nrows = 100)
    if (nrow(existing_data) > 0) {
      # Check latest datetime in existing data
      datetime_col <- ifelse("fint" %in% names(existing_data), "fint", "datetime")
      if (datetime_col %in% names(existing_data)) {
        latest_time <- max(as_datetime(existing_data[[datetime_col]]), na.rm = TRUE)
        hours_since_latest <- as.numeric(difftime(Sys.time(), latest_time, units = "hours"))
        cat("Latest data timestamp:", latest_time, "(", round(hours_since_latest, 1), "hours ago)\n")
        
        if (hours_since_latest < 3) {  # Very recent data
          needs_update <- FALSE
          cat("Data is very recent, skipping collection\n")
        }
      }
    }
  }
}

if (needs_update) {
  cat("Collecting new hourly data...\n")
  
  # Run the hourly data collection
  tryCatch({
    source("scripts/r/get_latest_data.R")
    cat("✅ Hourly collection completed\n")
  }, error = function(e) {
    cat("❌ Hourly collection failed:", e$message, "\n")
    
    # If collection failed but we have existing data, use it
    if (!is.na(existing_file)) {
      cat("Using existing hourly data despite collection failure\n")
    } else {
      stop("No hourly data available and collection failed")
    }
  })
}

# Load the hourly data
hourly_file <- hourly_files[file.exists(hourly_files)][1]
if (is.na(hourly_file)) {
  stop("No hourly data file found after collection attempt")
}

cat("Loading hourly data from:", basename(hourly_file), "\n")
hourly_data <- fread(hourly_file)

cat("Hourly data loaded:", nrow(hourly_data), "records\n")
cat("Columns:", paste(names(hourly_data), collapse = ", "), "\n")

# Basic validation
datetime_col <- ifelse("fint" %in% names(hourly_data), "fint", "datetime")
station_col <- ifelse("idema" %in% names(hourly_data), "idema", "indicativo")

if (!datetime_col %in% names(hourly_data)) {
  stop("No datetime column found in hourly data")
}
if (!station_col %in% names(hourly_data)) {
  stop("No station ID column found in hourly data")
}

# Add metadata if not present
if (!"collection_timestamp" %in% names(hourly_data)) {
  hourly_data$collection_timestamp <- Sys.time()
}
if (!"data_source" %in% names(hourly_data)) {
  hourly_data$data_source <- "aemet_hourly_api"
}

# Ensure datetime is properly formatted
hourly_data[[datetime_col]] <- as_datetime(hourly_data[[datetime_col]])

# Sort by station and datetime
hourly_data <- hourly_data[order(get(station_col), get(datetime_col))]

# Save as hourly_station_ongoing.csv.gz (ensure we have the .gz version)
output_file <- "data/output/hourly_station_ongoing.csv.gz"
fwrite(hourly_data, output_file)

# Also save uncompressed version for compatibility
output_file_csv <- "data/output/hourly_station_ongoing.csv"
fwrite(hourly_data, output_file_csv)

cat("\n=== Hourly Collection Complete ===\n")
cat("Output file:", output_file, "\n")
cat("Records:", nrow(hourly_data), "\n")
cat("File size:", round(file.size(output_file)/1024/1024, 1), "MB\n")

if (nrow(hourly_data) > 0) {
  cat("Date range:", min(as.Date(hourly_data[[datetime_col]]), na.rm = TRUE), 
      "to", max(as.Date(hourly_data[[datetime_col]]), na.rm = TRUE), "\n")
  cat("Stations:", length(unique(hourly_data[[station_col]])), "\n")
  
  # Show data freshness
  latest_obs <- max(hourly_data[[datetime_col]], na.rm = TRUE)
  hours_behind <- as.numeric(difftime(Sys.time(), latest_obs, units = "hours"))
  cat("Latest observation:", latest_obs, "(", round(hours_behind, 1), "hours ago)\n")
}
