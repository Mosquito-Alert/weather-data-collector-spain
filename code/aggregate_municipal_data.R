#!/usr/bin/env Rscript

# aggregate_municipal_data.R
# --------------------------
# Purpose: Create municipal-level weather data combining observations and forecasts
#
# This script aggregates weather station data to municipality level and combines it
# with municipal forecasts to provide a complete time series from 2013 through 7-day forecasts.
#
# Data Sources:
#   1. Daily aggregated station data (historical + current)
#   2. Municipal 7-day forecasts  
#   3. Municipality-station mapping (simplified approach using geographic proximity)
#
# Output: Municipal daily weather data including historical observations and forecasts
#
# Author: John Palmer
# Date: 2025-08-20

rm(list=ls())

# Dependencies ####
library(tidyverse)
library(lubridate)
library(data.table)

cat("=== MUNICIPAL DATA AGGREGATION ===\n")

# Check required input files
required_files = c(
  "data/output/daily_station_aggregated.csv.gz",           # From aggregate_daily_station_data.R
  "data/output/municipal_forecasts_2025-08-22.csv"        # From get_forecast_data.R (today's date)
)

# Also check for any recent forecast files if today's doesn't exist
if(!file.exists("data/output/municipal_forecasts_2025-08-22.csv")) {
  recent_forecast_files = list.files("data/output", pattern = "municipal_forecasts_.*\\.csv$", full.names = TRUE)
  if(length(recent_forecast_files) > 0) {
    # Use the most recent forecast file
    required_files[2] = tail(recent_forecast_files[order(file.mtime(recent_forecast_files))], 1)
    cat("Using recent forecast file:", required_files[2], "\n")
  }
}

missing_files = required_files[!file.exists(required_files)]
if(length(missing_files) > 0) {
  cat("ERROR: Missing required files:\n")
  for(file in missing_files) cat("  -", file, "\n")
  cat("Run the preceding scripts first.\n")
  quit(save="no", status=1)
}

# Load daily aggregated station data
cat("Loading daily aggregated station data...\n")
station_daily = fread("data/output/daily_station_aggregated.csv.gz")
station_daily$date = as.Date(station_daily$date)

cat("Loaded", nrow(station_daily), "daily station records.\n")
cat("Station data date range:", min(station_daily$date, na.rm=TRUE), "to", max(station_daily$date, na.rm=TRUE), "\n")
cat("Number of stations:", length(unique(station_daily$idema)), "\n")

# Load municipal forecasts
cat("Loading municipal forecast data...\n")
municipal_forecasts = fread(required_files[2])  # Use the forecast file we determined above
if("fecha" %in% names(municipal_forecasts)) {
  municipal_forecasts$fecha = as.Date(municipal_forecasts$fecha)
} else if("date" %in% names(municipal_forecasts)) {
  names(municipal_forecasts)[names(municipal_forecasts) == "date"] = "fecha"
  municipal_forecasts$fecha = as.Date(municipal_forecasts$fecha)
}

cat("Loaded", nrow(municipal_forecasts), "municipal forecast records.\n")
cat("Forecast date range:", min(municipal_forecasts$fecha, na.rm=TRUE), "to", max(municipal_forecasts$fecha, na.rm=TRUE), "\n")
cat("Number of municipalities:", length(unique(municipal_forecasts$municipio_id)), "\n")

# Load station-municipality mapping table
cat("Loading station-municipality mapping...\n")

if(!file.exists("data/input/station_point_municipaities_table.csv")) {
  cat("ERROR: Station-municipality mapping file not found: data/input/station_point_municipaities_table.csv\n")
  quit(save="no", status=1)
}

station_municipality_map = fread("data/input/station_point_municipaities_table.csv")
cat("Loaded mapping for", nrow(station_municipality_map), "stations to municipalities.\n")
cat("Number of municipalities:", length(unique(station_municipality_map$NATCODE)), "\n")

# Create proper municipality-station aggregation
cat("Aggregating station data by municipality...\n")
# Join station data with municipality mapping
cat("Joining station data with municipality mapping...\n")

# Merge station data with municipality mapping
station_daily_with_municipality = merge(
  station_daily,
  station_municipality_map[, .(idema = INDICATIVO, municipio_id = NATCODE, municipio_nombre = NAMEUNIT)],
  by = "idema",
  all.x = TRUE  # Keep all station data, even if not mapped
)

cat("Stations with municipality mapping:", 
    length(unique(station_daily_with_municipality$idema[!is.na(station_daily_with_municipality$municipio_id)])), "\n")
cat("Stations without mapping:", 
    length(unique(station_daily_with_municipality$idema[is.na(station_daily_with_municipality$municipio_id)])), "\n")

# Create municipal aggregates
cat("Creating municipal aggregates from station data...\n")

municipal_daily = station_daily_with_municipality[!is.na(municipio_id), .(
  value = mean(value, na.rm = TRUE),
  n_stations = length(unique(idema)),
  source = "station_aggregate"
), by = .(date, municipio_id, municipio_nombre, measure)]

cat("Created municipal daily aggregates:\n")
cat("  Records:", nrow(municipal_daily), "\n")
cat("  Municipalities:", length(unique(municipal_daily$municipio_id)), "\n")
cat("  Date range:", min(municipal_daily$date), "to", max(municipal_daily$date), "\n")

# Convert forecast data to compatible format
cat("Processing municipal forecast data...\n")

# Reshape forecast data to match station data format
forecast_reshaped = municipal_forecasts %>%
  select(municipio_id, municipio_nombre, fecha, 
         temperatura_maxima, temperatura_minima, temperatura_dato,
         humedad_maxima, humedad_minima, humedad_dato,
         prob_precipitacion, racha_max) %>%
  pivot_longer(cols = c(temperatura_maxima, temperatura_minima, temperatura_dato,
                       humedad_maxima, humedad_minima, humedad_dato,
                       prob_precipitacion, racha_max),
               names_to = "measure", values_to = "value") %>%
  filter(!is.na(value)) %>%
  mutate(
    date = fecha,
    # Map forecast variables to station variable names
    measure = case_when(
      measure == "temperatura_maxima" ~ "tamax",
      measure == "temperatura_minima" ~ "tamin", 
      measure == "temperatura_dato" ~ "ta",
      measure == "humedad_maxima" ~ "hr",  # Use hr for main humidity
      measure == "humedad_minima" ~ "hr_min",
      measure == "humedad_dato" ~ "hr_mean",
      measure == "prob_precipitacion" ~ "prec_prob",
      measure == "racha_max" ~ "vv",  # Map to wind speed variable
      TRUE ~ measure
    ),
    source = "municipal_forecast"
  ) %>%
  select(date, municipio_id, municipio_nombre, measure, value, source) %>%
  as.data.table()

cat("Reshaped forecast data:\n")
cat("  Records:", nrow(forecast_reshaped), "\n")
cat("  Variables:", paste(unique(forecast_reshaped$measure), collapse=", "), "\n")

# Match forecast data with municipal aggregates  
cat("Combining municipal station data with forecasts...\n")

# Filter forecast data to only municipalities that have station data
available_municipalities = unique(municipal_daily$municipio_id)
forecast_filtered = forecast_reshaped[municipio_id %in% available_municipalities]

cat("Municipalities with both station data and forecasts:", 
    length(intersect(unique(municipal_daily$municipio_id), unique(forecast_filtered$municipio_id))), "\n")

# Find the overlap/gap between station data and forecasts by municipality
overlap_summary = municipal_daily[, .(
  station_end_date = max(date, na.rm=TRUE),
  station_start_date = min(date, na.rm=TRUE)
), by = municipio_id]

forecast_summary = forecast_filtered[, .(
  forecast_start_date = min(date, na.rm=TRUE),
  forecast_end_date = max(date, na.rm=TRUE)  
), by = municipio_id]

coverage_summary = merge(overlap_summary, forecast_summary, by = "municipio_id", all = TRUE)
cat("Coverage summary for municipalities:\n")
print(coverage_summary[1:10])  # Show first 10 for brevity

# Combine municipal station data with forecasts
combined_municipal = rbind(
  municipal_daily[, .(date, municipio_id, municipio_nombre, measure, value, source)],
  forecast_filtered[, .(date, municipio_id, municipio_nombre, measure, value, source)],
  fill = TRUE
)

# Sort by date and measure
combined_municipal = combined_municipal[order(date, measure)]

# Create summary
cat("\n=== MUNICIPAL AGGREGATION SUMMARY ===\n")
cat("Total municipal records:", nrow(combined_municipal), "\n")
cat("Number of municipalities:", length(unique(combined_municipal$municipio_id)), "\n")
cat("Date range:", min(combined_municipal$date, na.rm=TRUE), "to", max(combined_municipal$date, na.rm=TRUE), "\n")
cat("Variables included:", paste(unique(combined_municipal$measure), collapse=", "), "\n")

# Summary by source
source_summary = combined_municipal[, .(
  records = .N,
  municipalities = length(unique(municipio_id)),
  date_min = min(date, na.rm=TRUE),
  date_max = max(date, na.rm=TRUE)
), by = source]

cat("\nBy source:\n")
print(source_summary)

# Summary by municipality (top 10 by record count)
municipality_summary = combined_municipal[, .(
  records = .N,
  variables = length(unique(measure)),
  date_min = min(date, na.rm=TRUE),
  date_max = max(date, na.rm=TRUE)
), by = .(municipio_id, municipio_nombre)][order(-records)]

cat("\nTop 10 municipalities by record count:\n")
print(municipality_summary[1:10])

# Summary by variable
variable_summary = combined_municipal[, .(
  records = .N,
  municipalities = length(unique(municipio_id)),
  date_min = min(date, na.rm=TRUE),
  date_max = max(date, na.rm=TRUE)
), by = measure]

cat("\nBy variable:\n")
print(variable_summary)

# Save the combined municipal data
output_file = "data/output/municipal_combined.csv.gz"
fwrite(combined_municipal, output_file)

cat("\n=== AGGREGATION COMPLETE ===\n")
cat("Municipal aggregated data saved to:", output_file, "\n")
cat("File size:", round(file.size(output_file)/1024/1024, 1), "MB\n")
cat("Total municipalities:", length(unique(combined_municipal$municipio_id)), "\n")
cat("Date coverage:", min(combined_municipal$date, na.rm=TRUE), "to", max(combined_municipal$date, na.rm=TRUE), "\n")

cat("\n=== MUNICIPAL AGGREGATION COMPLETE ===\n")
cat("Municipal combined data saved to:", output_file, "\n")
cat("File size:", round(file.size(output_file)/1024/1024, 1), "MB\n")

cat("\nNOTE: This version uses simplified municipality mapping.\n")
cat("For production use, implement proper geographic station-municipality mapping.\n")
