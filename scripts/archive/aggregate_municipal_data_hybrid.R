#!/usr/bin/env Rscript

# aggregate_municipal_data_hybrid.R
# Hybrid-compatible version of municipal aggregation
# Works with hybrid forecast data structure

rm(list=ls())

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== HYBRID MUNICIPAL DATA AGGREGATION ===\n")

# Load daily aggregated station data 
station_files = list.files("data/output", pattern = "daily_station_aggregated_.*\\.csv", full.names = TRUE)
if(length(station_files) == 0) {
  cat("ERROR: No aggregated station data found. Run hybrid station aggregation first.\n")
  quit(save="no", status=1)
}

latest_station_file = station_files[which.max(file.mtime(station_files))]
cat("Loading station data from:", latest_station_file, "\n")
station_daily = fread(latest_station_file)
station_daily$date = as.Date(station_daily$date)

cat("Loaded", nrow(station_daily), "station daily records.\n")
cat("Station date range:", as.character(min(station_daily$date)), "to", as.character(max(station_daily$date)), "\n")

# Load municipal forecast data
forecast_files = list.files("data/output", pattern = "municipal_forecasts_.*\\.csv", full.names = TRUE)
municipal_forecasts = data.table()

if(length(forecast_files) > 0) {
  latest_forecast_file = forecast_files[which.max(file.mtime(forecast_files))]
  cat("Loading municipal forecasts from:", latest_forecast_file, "\n")
  municipal_forecasts = fread(latest_forecast_file)
  municipal_forecasts$fecha = as.Date(municipal_forecasts$fecha)
  
  cat("Loaded", nrow(municipal_forecasts), "forecast records.\n")
  cat("Forecast date range:", as.character(min(municipal_forecasts$fecha)), "to", as.character(max(municipal_forecasts$fecha)), "\n")
} else {
  cat("No municipal forecast data found.\n")
}

# Load station-municipality mapping
mapping_file = "data/input/station_point_municipaities_table.csv"
if(!file.exists(mapping_file)) {
  cat("ERROR: Station-municipality mapping not found:", mapping_file, "\n")
  quit(save="no", status=1)
}

station_mapping = fread(mapping_file)
cat("Loaded mapping for", nrow(station_mapping), "stations to municipalities.\n")

# Clean and standardize mapping
station_mapping = station_mapping %>%
  rename(idema = INDICATIVO, municipio_code = NATCODE) %>%
  mutate(municipio_code = as.character(municipio_code)) %>%
  filter(!is.na(idema), !is.na(municipio_code)) %>%
  as.data.table()

# Aggregate station data by municipality
cat("Aggregating station data by municipality...\n")

station_municipal = station_daily %>%
  left_join(station_mapping, by = "idema") %>%
  filter(!is.na(municipio_code)) %>%
  group_by(municipio_code, date) %>%
  summarise(
    tmed_municipal = mean(tmed, na.rm = TRUE),
    tmax_municipal = max(tmax, na.rm = TRUE),
    tmin_municipal = min(tmin, na.rm = TRUE),
    prec_municipal = mean(prec, na.rm = TRUE),
    velmedia_municipal = mean(velmedia, na.rm = TRUE),
    hrMedia_municipal = mean(hrMedia, na.rm = TRUE),
    presMax_municipal = max(presMax, na.rm = TRUE),
    presMin_municipal = min(presMin, na.rm = TRUE),
    n_stations = n(),
    .groups = "drop"
  ) %>%
  mutate(
    source = "station_aggregated",
    fecha = date
  ) %>%
  as.data.table()

cat("Created municipal station aggregates:\n")
cat("  Records:", nrow(station_municipal), "\n")
cat("  Municipalities:", length(unique(station_municipal$municipio_code)), "\n")

# Process municipal forecast data if available
forecast_municipal = data.table()
if(nrow(municipal_forecasts) > 0) {
  cat("Processing municipal forecast data...\n")
  
  # Standardize forecast data structure
  forecast_municipal = municipal_forecasts %>%
    mutate(
      municipio_code = as.character(municipio_id),
      tmed_municipal = temp_avg,
      tmax_municipal = temp_max,
      tmin_municipal = temp_min,
      hrMedia_municipal = (humid_max + humid_min) / 2,
      velmedia_municipal = wind_speed,
      source = "forecast"
    ) %>%
    select(
      municipio_code, fecha, tmed_municipal, tmax_municipal, tmin_municipal,
      hrMedia_municipal, velmedia_municipal, source, municipio_nombre, provincia
    ) %>%
    as.data.table()
  
  cat("Processed", nrow(forecast_municipal), "forecast records.\n")
}

# Combine station aggregates and forecasts
cat("Combining municipal data sources...\n")

if(nrow(forecast_municipal) > 0) {
  # Combine station aggregates and forecasts, avoiding date overlaps
  combined_municipal = rbind(
    station_municipal[, .(municipio_code, fecha, tmed_municipal, tmax_municipal, tmin_municipal, 
                          hrMedia_municipal, velmedia_municipal, source, n_stations)],
    forecast_municipal[, .(municipio_code, fecha, tmed_municipal, tmax_municipal, tmin_municipal,
                          hrMedia_municipal, velmedia_municipal, source, n_stations = NA_integer_)],
    fill = TRUE
  )
} else {
  combined_municipal = station_municipal
}

# Remove duplicates (prefer forecasts over station aggregates for same date)
combined_municipal = combined_municipal %>%
  arrange(municipio_code, fecha, desc(source == "forecast")) %>%
  distinct(municipio_code, fecha, .keep_all = TRUE) %>%
  as.data.table()

# Add quality control and metadata
combined_municipal[, `:=`(
  temp_range_ok = (tmax_municipal >= tmin_municipal) & 
                  (tmed_municipal >= tmin_municipal) & 
                  (tmed_municipal <= tmax_municipal),
  temp_realistic = (tmin_municipal >= -50) & (tmax_municipal <= 60),
  processed_at = Sys.time()
)]

# Final summary
cat("\n=== MUNICIPAL AGGREGATION SUMMARY ===\n")
cat("Total municipal-days:", nrow(combined_municipal), "\n")
cat("Unique municipalities:", length(unique(combined_municipal$municipio_code)), "\n")
cat("Date range:", as.character(min(combined_municipal$fecha)), "to", as.character(max(combined_municipal$fecha)), "\n")

# Source breakdown
source_summary = combined_municipal[, .N, by = source]
print(source_summary)

cat("Temperature QC pass rate:", round(100 * mean(combined_municipal$temp_range_ok, na.rm=TRUE), 1), "%\n")

# Save results
output_file = paste0("data/output/municipal_aggregated_", Sys.Date(), ".csv")
write.csv(combined_municipal, output_file, row.names = FALSE)
cat("Municipal aggregation saved to:", output_file, "\n")

# Compressed version
output_file_gz = paste0(output_file, ".gz")
fwrite(combined_municipal, output_file_gz)
cat("Compressed version saved to:", output_file_gz, "\n")

# Show sample of final data
cat("\nSample of aggregated municipal data:\n")
print(head(combined_municipal[, .(municipio_code, fecha, tmed_municipal, tmax_municipal, tmin_municipal, source, temp_range_ok)], 5))

cat("\nMunicipal aggregation completed successfully.\n")
