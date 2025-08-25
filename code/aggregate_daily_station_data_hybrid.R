#!/usr/bin/env Rscript

# aggregate_daily_station_data_hybrid.R
# Hybrid-compatible version of daily station aggregation
# Works with both traditional data (idema) and hybrid data (indicativo)

rm(list=ls())

library(tidyverse)
library(lubridate) 
library(data.table)

cat("=== HYBRID DAILY STATION DATA AGGREGATION ===\n")

# Load recent station daily data from hybrid collection
station_daily_files = list.files("data/output", pattern = "station_daily_data_.*\\.csv", full.names = TRUE)
if(length(station_daily_files) == 0) {
  cat("ERROR: No station daily data files found. Run hybrid collection first.\n")
  quit(save="no", status=1)
}

# Use the most recent file
latest_file = station_daily_files[which.max(file.mtime(station_daily_files))]
cat("Loading station daily data from:", latest_file, "\n")

station_daily = fread(latest_file)
cat("Loaded", nrow(station_daily), "station daily records.\n")

# Standardize column names for compatibility
if("indicativo" %in% names(station_daily)) {
  station_daily$idema = station_daily$indicativo
}
if("fecha" %in% names(station_daily)) {
  station_daily$date = as.Date(station_daily$fecha)
}

# Check if we have hourly data for recent aggregation
hourly_file = "data/output/hourly_station_ongoing.csv.gz"
if(file.exists(hourly_file)) {
  cat("Loading hourly data for additional aggregation...\n")
  hourly_data = fread(hourly_file)
  hourly_data$fint = as_datetime(hourly_data$fint)
  hourly_data$date = as.Date(hourly_data$fint)
  
  cat("Hourly data date range:", min(hourly_data$date, na.rm=TRUE), "to", max(hourly_data$date, na.rm=TRUE), "\n")
  
  # Aggregate hourly to daily for recent days
  cat("Reshaping hourly data to wide format...\n")
  hourly_wide = hourly_data %>%
    select(date, idema, measure, value) %>%
    pivot_wider(names_from = measure, values_from = value, values_fn = mean) %>%
    as.data.table()
  
  cat("Aggregating", nrow(hourly_wide), "station-days from hourly data...\n")
  
  recent_daily = hourly_wide %>%
    group_by(date, idema) %>%
    summarise(
      tmed = mean(ta, na.rm = TRUE),
      tmax = max(coalesce(tamax, ta), na.rm = TRUE), 
      tmin = min(coalesce(tamin, ta), na.rm = TRUE),
      prec = sum(prec, na.rm = TRUE),
      velmedia = mean(vv, na.rm = TRUE),
      hrMedia = mean(hr, na.rm = TRUE),
      presMax = max(pres, na.rm = TRUE),
      presMin = min(pres, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      fecha = as.character(date),
      source = "hourly_aggregated"
    ) %>%
    as.data.table()
  
  cat("Aggregated", nrow(recent_daily), "station-days from hourly data.\n")
  
  # Combine with station daily data, avoiding duplicates
  if("date" %in% names(station_daily)) {
    existing_dates = unique(station_daily$date)
    recent_daily = recent_daily[!date %in% existing_dates]
    cat("Added", nrow(recent_daily), "new station-days from hourly aggregation.\n")
  }
  
  # Merge the datasets
  combined_daily = rbind(
    station_daily[, .(date, idema, tmed, tmax, tmin, prec, velmedia, hrMedia, presMax, presMin, fecha)],
    recent_daily[, .(date, idema, tmed, tmax, tmin, prec, velmedia, hrMedia, presMax, presMin, fecha)],
    fill = TRUE
  )
  
} else {
  cat("No hourly data found. Using only station daily data.\n")
  combined_daily = station_daily
}

# Clean and standardize the data
cat("Cleaning and standardizing data...\n")
final_daily = combined_daily %>%
  filter(!is.na(date), !is.na(idema)) %>%
  arrange(idema, date) %>%
  distinct(idema, date, .keep_all = TRUE) %>%  # Remove any duplicates
  mutate(
    # Ensure numeric columns are properly formatted
    tmed = as.numeric(tmed),
    tmax = as.numeric(tmax),
    tmin = as.numeric(tmin), 
    prec = as.numeric(prec),
    velmedia = as.numeric(velmedia),
    hrMedia = as.numeric(hrMedia),
    presMax = as.numeric(presMax),
    presMin = as.numeric(presMin)
  ) %>%
  as.data.table()

# Add quality control flags
final_daily[, `:=`(
  temp_range_ok = (tmax >= tmin) & (tmed >= tmin) & (tmed <= tmax),
  temp_realistic = (tmin >= -50) & (tmax <= 60),
  prec_realistic = (prec >= 0) & (prec <= 500),
  processed_at = Sys.time()
)]

# Summary statistics
cat("\n=== AGGREGATION SUMMARY ===\n")
cat("Total station-days:", nrow(final_daily), "\n")
cat("Unique stations:", length(unique(final_daily$idema)), "\n")
cat("Date range:", as.character(min(final_daily$date)), "to", as.character(max(final_daily$date)), "\n")
cat("Temperature QC pass rate:", round(100 * mean(final_daily$temp_range_ok, na.rm=TRUE), 1), "%\n")

# Save results
output_file = paste0("data/output/daily_station_aggregated_", Sys.Date(), ".csv")
write.csv(final_daily, output_file, row.names = FALSE)
cat("Daily station aggregation saved to:", output_file, "\n")

# Compressed version
output_file_gz = paste0(output_file, ".gz")
fwrite(final_daily, output_file_gz)
cat("Compressed version saved to:", output_file_gz, "\n")

# Show sample of final data
cat("\nSample of aggregated data:\n")
print(head(final_daily[, .(date, idema, tmed, tmax, tmin, prec, temp_range_ok)], 5))

cat("\nAggregation completed successfully.\n")
