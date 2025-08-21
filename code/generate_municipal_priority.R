#!/usr/bin/env Rscript

# generate_municipal_priority.R
# ------------------------------
# Purpose: Priority generation of municipal daily data for immediate modeling use
# 
# Strategy: Start from present/forecast and work backwards, saving incrementally
# This ensures models can start using data immediately while historical collection continues
#
# Priority Schedule:
# 1. Generate forecast period (next 7 days) - IMMEDIATE
# 2. Generate recent period (past 7 days) - HIGH PRIORITY  
# 3. Generate historical chunks working backwards - BACKGROUND
#
# Output: daily_municipal_extended.csv.gz (updated incrementally)

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== MUNICIPAL DATA PRIORITY GENERATION ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Configuration
FORECAST_DAYS <- 7
RECENT_DAYS <- 7
HISTORICAL_CHUNK_DAYS <- 30  # Process historical data in monthly chunks
OUTPUT_FILE <- "data/output/daily_municipal_extended.csv.gz"

# Phase 1: Generate forecast data (IMMEDIATE - for models)
generate_forecast_period <- function() {
  cat("\n=== PHASE 1: FORECAST PERIOD (IMMEDIATE PRIORITY) ===\n")
  
  # Check if we have recent municipal forecast data
  forecast_file <- list.files("data/output", pattern = "municipal_forecasts_.*\\.csv", full.names = TRUE)
  
  if (length(forecast_file) > 0) {
    cat("Found forecast file:", basename(forecast_file[1]), "\n")
    forecast_data <- fread(forecast_file[1])
    
    # Process and standardize forecast data
    cat("Processing forecast data for", nrow(forecast_data), "records\n")
    
    # Save forecast portion immediately for models
    forecast_output <- paste0("data/output/daily_municipal_forecast_only.csv.gz")
    fwrite(forecast_data, forecast_output)
    cat("✅ IMMEDIATE: Forecast data available at:", forecast_output, "\n")
    
    return(forecast_data)
  } else {
    cat("❌ No forecast data found. Run get_forecast_data.R first.\n")
    return(NULL)
  }
}

# Phase 2: Generate recent period (HIGH PRIORITY - past week)
generate_recent_period <- function() {
  cat("\n=== PHASE 2: RECENT PERIOD (HIGH PRIORITY) ===\n")
  
  recent_start <- Sys.Date() - RECENT_DAYS
  recent_end <- Sys.Date() - 1
  
  cat("Generating recent period:", as.character(recent_start), "to", as.character(recent_end), "\n")
  
  # This will aggregate station data to municipal level for recent days
  # Implementation needed: municipality-station mapping and aggregation
  
  cat("🔄 Recent period generation - implementation needed\n")
  cat("Will aggregate station observations to municipal level\n")
  
  return(NULL)
}

# Phase 3: Generate historical chunks (BACKGROUND - work backwards)
generate_historical_chunks <- function(start_date = Sys.Date() - RECENT_DAYS - 1) {
  cat("\n=== PHASE 3: HISTORICAL CHUNKS (BACKGROUND) ===\n")
  
  chunk_end <- start_date
  chunk_start <- chunk_end - HISTORICAL_CHUNK_DAYS
  
  cat("Next historical chunk:", as.character(chunk_start), "to", as.character(chunk_end), "\n")
  cat("🔄 Historical chunk generation - implementation needed\n")
  
  return(NULL)
}

# Incremental save function
save_incremental_update <- function(new_data, existing_file = OUTPUT_FILE) {
  if (file.exists(existing_file)) {
    existing_data <- fread(existing_file)
    combined_data <- rbind(existing_data, new_data)
    # Remove duplicates and sort by date
    combined_data <- combined_data[!duplicated(combined_data[, .(municipio_id, fecha)])]
    combined_data <- combined_data[order(municipio_id, fecha)]
  } else {
    combined_data <- new_data
  }
  
  fwrite(combined_data, existing_file)
  cat("💾 Incremental save completed:", nrow(combined_data), "total records\n")
  
  return(combined_data)
}

# Main execution function
main <- function(phase = "all") {
  
  if (phase %in% c("all", "forecast")) {
    forecast_data <- generate_forecast_period()
    if (!is.null(forecast_data)) {
      # Immediately save forecast data for model use
      save_incremental_update(forecast_data)
    }
  }
  
  if (phase %in% c("all", "recent")) {
    recent_data <- generate_recent_period()
    if (!is.null(recent_data)) {
      save_incremental_update(recent_data)
    }
  }
  
  if (phase %in% c("all", "historical")) {
    historical_data <- generate_historical_chunks()
    if (!is.null(historical_data)) {
      save_incremental_update(historical_data)
    }
  }
  
  cat("\n=== PRIORITY GENERATION SUMMARY ===\n")
  if (file.exists(OUTPUT_FILE)) {
    final_data <- fread(OUTPUT_FILE)
    cat("Total records in municipal dataset:", nrow(final_data), "\n")
    cat("Date range:", min(final_data$fecha, na.rm = TRUE), "to", max(final_data$fecha, na.rm = TRUE), "\n")
    cat("Municipalities covered:", length(unique(final_data$municipio_id)), "\n")
  }
  
  cat("Municipal data ready for modeling at:", OUTPUT_FILE, "\n")
}

# Allow script to be run with different phases
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  main(args[1])
} else {
  main("forecast")  # Default: just generate forecast for immediate model use
}

cat("Priority municipal data generation completed at:", format(Sys.time()), "\n")
