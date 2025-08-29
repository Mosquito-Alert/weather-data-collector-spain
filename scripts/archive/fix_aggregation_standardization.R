#!/usr/bin/env Rscript

# Fixed collection script that applies standardization during aggregation
# This prevents duplicate columns with different names for same concept

rm(list=ls())

library(tidyverse)
library(lubridate)
library(data.table)

# Load standardization functions
source("scripts/r/variable_standardization_functions.R")

cat("=======================================\n")
cat("PROPERLY STANDARDIZED DATA COLLECTION\n") 
cat("=======================================\n")
cat("Started at:", format(Sys.time()), "\n\n")

start_time <- Sys.time()

# === DATASET 1: DAILY STATION HISTORICAL ===
cat("=== DATASET 1: DAILY STATION HISTORICAL ===\n")

# Simulate loading station data and apply standardization immediately
tryCatch({
  # Check if we have any existing data to work with
  if (file.exists("data/output/daily_station_historical.csv")) {
    # Load existing data  
    station_data <- fread("data/output/daily_station_historical.csv")
    cat("Loaded existing station data with", nrow(station_data), "rows\n")
    
    # Apply proper standardization
    station_mapping <- get_standard_station_mapping()
    station_data_clean <- standardize_columns(station_data, station_mapping)
    
    # Remove any duplicate columns (keep first occurrence)
    dup_cols <- duplicated(colnames(station_data_clean))
    if (any(dup_cols)) {
      cat("Removing", sum(dup_cols), "duplicate columns\n")
      station_data_clean <- station_data_clean[, !dup_cols, drop = FALSE]
    }
    
    # Create clean backup
    backup_file <- paste0("data/output/daily_station_historical.csv.backup_clean_", 
                         format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy("data/output/daily_station_historical.csv", backup_file)
    
    # Write cleaned version
    fwrite(station_data_clean, "data/output/daily_station_historical.csv")
    cat("✅ Dataset 1 cleaned and standardized\n\n")
  } else {
    cat("❌ No existing station data found\n\n")
  }
}, error = function(e) {
  cat("❌ Dataset 1 failed:", e$message, "\n\n")
})

# === DATASET 2: DAILY MUNICIPAL EXTENDED ===
cat("=== DATASET 2: DAILY MUNICIPAL EXTENDED ===\n")

tryCatch({
  if (file.exists("data/output/daily_municipal_extended.csv")) {
    # Load existing municipal data
    municipal_data <- fread("data/output/daily_municipal_extended.csv")
    cat("Loaded existing municipal data with", nrow(municipal_data), "rows\n")
    
    # Apply proper standardization  
    municipal_mapping <- get_standard_municipal_mapping()
    municipal_data_clean <- standardize_columns(municipal_data, municipal_mapping)
    
    # Remove duplicate columns
    dup_cols <- duplicated(colnames(municipal_data_clean))
    if (any(dup_cols)) {
      cat("Removing", sum(dup_cols), "duplicate columns\n")
      municipal_data_clean <- municipal_data_clean[, !dup_cols, drop = FALSE]
    }
    
    # Handle multiple municipality ID columns by keeping the first non-empty one
    muni_id_cols <- grep("municipality_id", colnames(municipal_data_clean), value = TRUE)
    if (length(muni_id_cols) > 1) {
      cat("Found multiple municipality ID columns:", paste(muni_id_cols, collapse = ", "), "\n")
      
      # Create consolidated municipality_id column
      municipal_data_clean$municipality_id_final <- NA
      for (col in muni_id_cols) {
        missing_mask <- is.na(municipal_data_clean$municipality_id_final)
        municipal_data_clean$municipality_id_final[missing_mask] <- 
          municipal_data_clean[[col]][missing_mask]
      }
      
      # Remove old columns and rename final one
      municipal_data_clean <- municipal_data_clean[, !colnames(municipal_data_clean) %in% muni_id_cols, drop = FALSE]
      colnames(municipal_data_clean)[colnames(municipal_data_clean) == "municipality_id_final"] <- "municipality_id"
      cat("Consolidated municipality ID columns\n")
    }
    
    # Create clean backup
    backup_file <- paste0("data/output/daily_municipal_extended.csv.backup_clean_", 
                         format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy("data/output/daily_municipal_extended.csv", backup_file)
    
    # Write cleaned version
    fwrite(municipal_data_clean, "data/output/daily_municipal_extended.csv")
    cat("✅ Dataset 2 cleaned and standardized\n\n")
  } else {
    cat("❌ No existing municipal data found\n\n")
  }
}, error = function(e) {
  cat("❌ Dataset 2 failed:", e$message, "\n\n")
})

# === DATASET 3: HOURLY STATION ONGOING ===
cat("=== DATASET 3: HOURLY STATION ONGOING ===\n")

tryCatch({
  if (file.exists("data/output/hourly_station_ongoing.csv")) {
    # Load existing hourly data
    hourly_data <- fread("data/output/hourly_station_ongoing.csv")
    cat("Loaded existing hourly data with", nrow(hourly_data), "rows\n")
    
    # Apply proper standardization
    hourly_mapping <- list(
      "idema" = "station_id",
      "fint" = "datetime", 
      "date" = "date",
      "measure" = "variable_type",
      "value" = "value"
    )
    
    hourly_data_clean <- standardize_columns(hourly_data, hourly_mapping)
    
    # Remove duplicate columns
    dup_cols <- duplicated(colnames(hourly_data_clean))
    if (any(dup_cols)) {
      cat("Removing", sum(dup_cols), "duplicate columns\n")
      hourly_data_clean <- hourly_data_clean[, !dup_cols, drop = FALSE]
    }
    
    # Create clean backup
    backup_file <- paste0("data/output/hourly_station_ongoing.csv.backup_clean_", 
                         format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy("data/output/hourly_station_ongoing.csv", backup_file)
    
    # Write cleaned version
    fwrite(hourly_data_clean, "data/output/hourly_station_ongoing.csv")
    cat("✅ Dataset 3 cleaned and standardized\n\n")
  } else {
    cat("❌ No existing hourly data found\n\n")
  }
}, error = function(e) {
  cat("❌ Dataset 3 failed:", e$message, "\n\n")
})

# === VERIFICATION ===
cat("=== FINAL VERIFICATION ===\n")

datasets <- c("daily_station_historical.csv", "daily_municipal_extended.csv", "hourly_station_ongoing.csv")

for (dataset in datasets) {
  file_path <- file.path("data/output", dataset)
  if (file.exists(file_path)) {
    data <- fread(file_path, nrows = 1)
    cat(dataset, ":\n")
    cat("  Columns:", ncol(data), "\n")
    cat("  Column names:", paste(head(colnames(data), 10), collapse = ", "), "...\n")
    
    # Check for numbered columns (sign of duplicates)
    numbered_cols <- grep("\\.[0-9]+$", colnames(data), value = TRUE)
    if (length(numbered_cols) > 0) {
      cat("  ❌ WARNING: Found numbered columns:", paste(numbered_cols, collapse = ", "), "\n")
    } else {
      cat("  ✅ No numbered columns found\n")
    }
    cat("\n")
  }
}

end_time <- Sys.time()
total_time <- as.numeric(difftime(end_time, start_time, units = "mins"))

cat("=======================================\n")
cat("STANDARDIZATION COMPLETED\n")
cat("Total time:", round(total_time, 2), "minutes\n")
cat("=======================================\n")
