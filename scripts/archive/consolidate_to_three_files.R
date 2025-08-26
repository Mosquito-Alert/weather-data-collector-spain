#!/usr/bin/env Rscript

# consolidate_to_three_files.R
# Consolidates all fragmented data files into three final datasets
# Implements data priority logic (actual > forecast) for municipal data

rm(list=ls())

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== CONSOLIDATING TO THREE FINAL DATASETS ===\n")
cat("Target files:\n")
cat("  1. daily_station_historical.csv\n")
cat("  2. daily_municipal_extended.csv (with priority logic)\n") 
cat("  3. hourly_station_ongoing.csv\n\n")

# ====================================================================
# DATASET 1: DAILY STATION HISTORICAL DATA
# ====================================================================
cat("=== CONSOLIDATING DAILY STATION DATA ===\n")

station_files = list.files("data/output", pattern = "station_daily_data_.*\\.csv", full.names = TRUE)
station_files = c(station_files, list.files("data/output", pattern = "daily_station_aggregated.*\\.csv", full.names = TRUE))

if(length(station_files) > 0) {
  station_data = data.table()
  
  for(file in station_files) {
    cat("Loading:", basename(file), "\n")
    temp_data = fread(file)
    
    # Standardize column names
    if("indicativo" %in% names(temp_data) && !"idema" %in% names(temp_data)) {
      temp_data$idema = temp_data$indicativo
    }
    if("fecha" %in% names(temp_data) && !"date" %in% names(temp_data)) {
      temp_data$date = as.Date(temp_data$fecha)
    }
    
    station_data = rbind(station_data, temp_data, fill = TRUE)
  }
  
  # Remove duplicates (keep most recent processing)
  station_data$date = as.Date(station_data$date)
  station_data = station_data[!is.na(date) & !is.na(idema)]
  station_data = station_data[order(date, idema, -processed_at)]
  station_data = station_data[!duplicated(paste(date, idema))]
  
  # Save consolidated file
  output_file = "data/output/daily_station_historical.csv"
  fwrite(station_data, output_file)
  cat("✅ Consolidated", nrow(station_data), "station-day records to:", output_file, "\n")
  cat("   Date range:", as.character(min(station_data$date)), "to", as.character(max(station_data$date)), "\n")
  cat("   Unique stations:", length(unique(station_data$idema)), "\n\n")
} else {
  cat("⚠️  No station daily data files found\n\n")
}

# ====================================================================
# DATASET 2: DAILY MUNICIPAL EXTENDED (WITH PRIORITY LOGIC)
# ====================================================================
cat("=== CONSOLIDATING MUNICIPAL DATA WITH PRIORITY LOGIC ===\n")

# Load all municipal forecast files
forecast_files = list.files("data/output", pattern = "municipal_forecasts_.*\\.csv", full.names = TRUE)
municipal_data = data.table()

if(length(forecast_files) > 0) {
  for(file in forecast_files) {
    cat("Loading forecast file:", basename(file), "\n")
    temp_data = fread(file)
    temp_data$data_source = "forecast"
    temp_data$fecha = as.Date(temp_data$fecha)
    municipal_data = rbind(municipal_data, temp_data, fill = TRUE)
  }
}

# Load aggregated municipal files (these contain station-based data)
municipal_agg_files = list.files("data/output", pattern = "municipal_aggregated_.*\\.csv", full.names = TRUE)
station_based_municipal = data.table()

if(length(municipal_agg_files) > 0) {
  for(file in municipal_agg_files) {
    cat("Loading station-based municipal file:", basename(file), "\n")
    temp_data = fread(file)
    temp_data$data_source = "station_aggregated"
    
    # Handle date column (might be 'date' or 'fecha')
    if("date" %in% names(temp_data) && !"fecha" %in% names(temp_data)) {
      temp_data$fecha = as.Date(temp_data$date)
    } else if("fecha" %in% names(temp_data)) {
      temp_data$fecha = as.Date(temp_data$fecha)
    }
    
    station_based_municipal = rbind(station_based_municipal, temp_data, fill = TRUE)
  }
}

# Combine and apply priority logic
if(nrow(municipal_data) > 0 || nrow(station_based_municipal) > 0) {
  # Standardize column names before combining
  if(nrow(municipal_data) > 0) {
    # Standardize forecast data columns
    if("municipio_id" %in% names(municipal_data)) {
      municipal_data$municipio = municipal_data$municipio_id
    }
  }
  
  if(nrow(station_based_municipal) > 0) {
    # Standardize station-based municipal data columns  
    if("municipio_code" %in% names(station_based_municipal)) {
      station_based_municipal$municipio = station_based_municipal$municipio_code
    }
  }
  
  # Combine all municipal data
  all_municipal = rbind(municipal_data, station_based_municipal, fill = TRUE)
  all_municipal = all_municipal[!is.na(fecha)]
  
  # Priority logic: station_aggregated > forecast
  # Sort by priority (station_aggregated comes first)
  priority_order = c("station_aggregated", "forecast")
  all_municipal$priority = match(all_municipal$data_source, priority_order)
  all_municipal = all_municipal[order(fecha, municipio, priority)]
  
  # Keep only the highest priority record for each date-municipality
  final_municipal = all_municipal[!duplicated(paste(fecha, municipio))]
  
  # Summary of data sources
  source_summary = final_municipal[, .N, by = data_source]
  cat("Data source composition:\n")
  print(source_summary)
  
  # Save consolidated file
  output_file = "data/output/daily_municipal_extended.csv"
  fwrite(final_municipal, output_file)
  cat("✅ Consolidated", nrow(final_municipal), "municipal-day records to:", output_file, "\n")
  cat("   Date range:", as.character(min(final_municipal$fecha)), "to", as.character(max(final_municipal$fecha)), "\n")
  cat("   Unique municipalities:", length(unique(final_municipal$municipio)), "\n\n")
} else {
  cat("⚠️  No municipal data files found\n\n")
}

# ====================================================================
# DATASET 3: HOURLY STATION ONGOING
# ====================================================================
cat("=== CONSOLIDATING HOURLY STATION DATA ===\n")

# Check if existing hourly file exists and load it
existing_hourly_file = "data/output/hourly_station_ongoing.csv.gz"
hourly_data = data.table()

if(file.exists(existing_hourly_file)) {
  cat("Loading existing hourly data from:", existing_hourly_file, "\n")
  hourly_data = fread(existing_hourly_file)
  hourly_data$date = as.Date(hourly_data$date)
  cat("Existing hourly records:", nrow(hourly_data), "\n")
}

# Load any recent hourly data files
recent_hourly_files = list.files("data/output", pattern = "latest_weather_.*\\.csv", full.names = TRUE)
if(length(recent_hourly_files) > 0) {
  for(file in recent_hourly_files) {
    cat("Loading recent hourly file:", basename(file), "\n")
    temp_data = fread(file)
    temp_data$date = as.Date(temp_data$date)
    hourly_data = rbind(hourly_data, temp_data, fill = TRUE)
  }
}

if(nrow(hourly_data) > 0) {
  # Handle different hourly data structures
  if("fint" %in% names(hourly_data) && !"date" %in% names(hourly_data)) {
    # Convert fint to date if needed
    hourly_data$date = as.Date(hourly_data$fint)
  }
  
  # Remove duplicates based on available columns
  hourly_data = hourly_data[!is.na(date) & !is.na(idema)]
  
  # For wide format hourly data, use different deduplication
  if("hora" %in% names(hourly_data)) {
    hourly_data = hourly_data[order(date, idema, hora, -processed_at)]
    hourly_data = hourly_data[!duplicated(paste(date, idema, hora))]
  } else {
    # For long format data (measure/value structure)
    if("measure" %in% names(hourly_data) && "value" %in% names(hourly_data)) {
      hourly_data = hourly_data[order(date, idema, measure, -fint)]
      hourly_data = hourly_data[!duplicated(paste(date, idema, measure, fint))]
    } else {
      # Basic deduplication
      hourly_data = hourly_data[order(date, idema)]
      hourly_data = hourly_data[!duplicated(paste(date, idema))]
    }
  }
  
  # Save consolidated file (compressed)
  output_file = "data/output/hourly_station_ongoing.csv"
  fwrite(hourly_data, output_file)
  
  output_file_gz = paste0(output_file, ".gz")
  fwrite(hourly_data, output_file_gz)
  
  cat("✅ Consolidated", nrow(hourly_data), "hourly records to:", output_file_gz, "\n")
  cat("   Date range:", as.character(min(hourly_data$date)), "to", as.character(max(hourly_data$date)), "\n")
  cat("   Unique stations:", length(unique(hourly_data$idema)), "\n\n")
} else {
  cat("⚠️  No hourly data found\n\n")
}

# ====================================================================
# CLEANUP RECOMMENDATIONS
# ====================================================================
cat("=== CONSOLIDATION COMPLETE ===\n")
cat("Three final datasets created:\n")
if(file.exists("data/output/daily_station_historical.csv")) {
  size_mb = round(file.size("data/output/daily_station_historical.csv") / 1024 / 1024, 2)
  cat("  ✅ daily_station_historical.csv (", size_mb, " MB)\n")
}
if(file.exists("data/output/daily_municipal_extended.csv")) {
  size_mb = round(file.size("data/output/daily_municipal_extended.csv") / 1024 / 1024, 2)
  cat("  ✅ daily_municipal_extended.csv (", size_mb, " MB)\n")
}
if(file.exists("data/output/hourly_station_ongoing.csv.gz")) {
  size_mb = round(file.size("data/output/hourly_station_ongoing.csv.gz") / 1024 / 1024, 2)
  cat("  ✅ hourly_station_ongoing.csv.gz (", size_mb, " MB)\n")
}

cat("\nYou can now safely remove the dated fragment files:\n")
cat("  rm data/output/station_daily_data_*.csv*\n")
cat("  rm data/output/municipal_forecasts_*.csv*\n") 
cat("  rm data/output/municipal_aggregated_*.csv*\n")
cat("  rm data/output/daily_station_aggregated_*.csv*\n")
cat("  rm data/output/latest_weather_*.csv*\n")

cat("\nConsolidation completed successfully!\n")
