#!/usr/bin/env Rscript

# check_data_gaps.R
# Comprehensive gap detection and filling system for weather data
# Identifies missing data and creates targeted collection tasks

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== WEATHER DATA GAP ANALYSIS ===\n")
cat("Analysis started at:", format(Sys.time()), "\n\n")

# Configuration
ANALYSIS_DATE = Sys.Date()
HISTORICAL_START = as.Date("2013-01-01")  # Expected historical coverage start
FORECAST_DAYS = 7  # Expected forecast coverage

# Helper function to create date ranges
create_date_range <- function(start_date, end_date) {
  seq.Date(from = start_date, to = end_date, by = "day")
}

# === 1. STATION DAILY DATA GAPS ===
cat("1. ANALYZING STATION DAILY DATA GAPS\n")
cat("=====================================\n")

# Load all available station daily data
station_files = list.files("data/output", pattern = "daily_station_aggregated_.*\\.csv", full.names = TRUE)
station_daily_gaps = data.table()

if(length(station_files) > 0) {
  # Combine all station daily files
  all_station_data = rbindlist(lapply(station_files, fread), fill = TRUE)
  all_station_data$date = as.Date(all_station_data$date)
  
  # Get unique stations and expected date range
  unique_stations = unique(all_station_data$idema)
  expected_dates = create_date_range(HISTORICAL_START, ANALYSIS_DATE - 1)
  
  cat("Stations with data:", length(unique_stations), "\n")
  cat("Expected date range:", as.character(min(expected_dates)), "to", as.character(max(expected_dates)), "\n")
  
  # Create complete grid and find gaps
  complete_grid = expand.grid(
    idema = unique_stations,
    date = expected_dates,
    stringsAsFactors = FALSE
  ) %>% as.data.table()
  
  # Identify gaps
  station_daily_gaps = complete_grid[!all_station_data[, .(idema, date)], on = .(idema, date)]
  
  cat("Total expected station-days:", nrow(complete_grid), "\n")
  cat("Available station-days:", nrow(all_station_data), "\n")
  cat("Missing station-days:", nrow(station_daily_gaps), "\n")
  cat("Coverage:", round(100 * nrow(all_station_data) / nrow(complete_grid), 1), "%\n\n")
  
  # Gap summary by station
  gap_by_station = station_daily_gaps[, .N, by = idema][order(-N)]
  cat("Top 10 stations with most missing days:\n")
  print(head(gap_by_station, 10))
  
} else {
  cat("No station daily data files found.\n")
}

cat("\n")

# === 2. MUNICIPAL FORECAST DATA GAPS ===
cat("2. ANALYZING MUNICIPAL FORECAST DATA GAPS\n")
cat("=========================================\n")

# Load municipal data
municipal_files = list.files("data/output", pattern = "municipal_aggregated_.*\\.csv", full.names = TRUE)
municipal_forecast_gaps = data.table()

if(length(municipal_files) > 0) {
  all_municipal_data = rbindlist(lapply(municipal_files, fread), fill = TRUE)
  all_municipal_data$fecha = as.Date(all_municipal_data$fecha)
  
  # Focus on forecast period (recent + future)
  forecast_start = ANALYSIS_DATE - 3  # Recent days
  forecast_end = ANALYSIS_DATE + FORECAST_DAYS
  forecast_dates = create_date_range(forecast_start, forecast_end)
  
  # Expected municipalities (load from input)
  if(file.exists("data/input/municipalities.csv.gz")) {
    all_municipalities = fread("data/input/municipalities.csv.gz")
    unique_municipalities = unique(as.character(all_municipalities$CUMUN))
    
    cat("Expected municipalities:", length(unique_municipalities), "\n")
    cat("Forecast period:", as.character(min(forecast_dates)), "to", as.character(max(forecast_dates)), "\n")
    
    # Create complete forecast grid
    forecast_grid = expand.grid(
      municipio_code = unique_municipalities,
      fecha = forecast_dates,
      stringsAsFactors = FALSE
    ) %>% as.data.table()
    
    # Identify forecast gaps
    available_forecasts = all_municipal_data[fecha %in% forecast_dates & source == "forecast", .(municipio_code, fecha)]
    municipal_forecast_gaps = forecast_grid[!available_forecasts, on = .(municipio_code, fecha)]
    
    cat("Total expected municipal forecasts:", nrow(forecast_grid), "\n")
    cat("Available forecasts:", nrow(available_forecasts), "\n")
    cat("Missing forecasts:", nrow(municipal_forecast_gaps), "\n")
    cat("Forecast coverage:", round(100 * nrow(available_forecasts) / nrow(forecast_grid), 1), "%\n\n")
    
    # Gap summary by municipality
    gap_by_municipality = municipal_forecast_gaps[, .N, by = municipio_code][order(-N)]
    cat("Top 10 municipalities with most missing forecasts:\n")
    print(head(gap_by_municipality, 10))
    
  } else {
    cat("Municipality list not found.\n")
  }
} else {
  cat("No municipal data files found.\n")
}

cat("\n")

# === 3. HOURLY DATA CONTINUITY ===
cat("3. ANALYZING HOURLY DATA CONTINUITY\n")
cat("===================================\n")

if(file.exists("data/output/hourly_station_ongoing.csv.gz")) {
  hourly_data = fread("data/output/hourly_station_ongoing.csv.gz")
  hourly_data$fint = as_datetime(hourly_data$fint)
  hourly_data$date = as.Date(hourly_data$fint)
  
  # Check recent continuity (last 30 days)
  recent_start = ANALYSIS_DATE - 30
  recent_hourly = hourly_data[date >= recent_start]
  
  # Count observations per day
  daily_counts = recent_hourly[, .N, by = date][order(date)]
  
  cat("Recent hourly data (last 30 days):\n")
  cat("Total observations:", nrow(recent_hourly), "\n")
  cat("Date range:", as.character(min(daily_counts$date)), "to", as.character(max(daily_counts$date)), "\n")
  cat("Average observations per day:", round(mean(daily_counts$N), 0), "\n")
  
  # Identify days with low counts (potential gaps)
  low_count_threshold = quantile(daily_counts$N, 0.25)  # Bottom quartile
  low_count_days = daily_counts[N < low_count_threshold]
  
  if(nrow(low_count_days) > 0) {
    cat("Days with potentially low observation counts:\n")
    print(head(low_count_days, 10))
  }
  
} else {
  cat("No hourly data file found.\n")
}

cat("\n")

# === 4. GENERATE GAP-FILLING RECOMMENDATIONS ===
cat("4. GAP-FILLING RECOMMENDATIONS\n")
cat("==============================\n")

recommendations = list()

# Station daily gaps
if(nrow(station_daily_gaps) > 0) {
  # Prioritize recent gaps and high-value stations
  priority_station_gaps = station_daily_gaps[date >= (ANALYSIS_DATE - 90)]  # Last 90 days
  
  if(nrow(priority_station_gaps) > 0) {
    recommendations$station_daily = list(
      priority = "HIGH",
      action = "Collect recent station daily data",
      gaps = nrow(priority_station_gaps),
      command = "Rscript code/get_station_daily_hybrid.R # Focus on recent dates"
    )
  }
}

# Municipal forecast gaps
if(nrow(municipal_forecast_gaps) > 0) {
  # Current and future forecasts are high priority
  current_forecast_gaps = municipal_forecast_gaps[fecha >= ANALYSIS_DATE]
  
  if(nrow(current_forecast_gaps) > 0) {
    recommendations$municipal_forecasts = list(
      priority = "CRITICAL",
      action = "Re-collect municipal forecasts",
      gaps = nrow(current_forecast_gaps),
      command = "Rscript code/get_forecast_data_hybrid.R"
    )
  }
}

# Print recommendations
if(length(recommendations) > 0) {
  for(i in seq_along(recommendations)) {
    rec = recommendations[[i]]
    cat("RECOMMENDATION", i, "- Priority:", rec$priority, "\n")
    cat("Action:", rec$action, "\n")
    cat("Gaps to fill:", rec$gaps, "\n")
    cat("Command:", rec$command, "\n\n")
  }
} else {
  cat("✅ No immediate gap-filling actions needed.\n\n")
}

# === 5. SAVE GAP ANALYSIS RESULTS ===
cat("5. SAVING GAP ANALYSIS RESULTS\n")
cat("==============================\n")

# Create gap analysis summary
gap_summary = list(
  analysis_date = ANALYSIS_DATE,
  station_daily = list(
    total_gaps = ifelse(exists("station_daily_gaps"), nrow(station_daily_gaps), 0),
    coverage_percent = ifelse(exists("complete_grid") && exists("all_station_data"), 
                             round(100 * nrow(all_station_data) / nrow(complete_grid), 1), 0)
  ),
  municipal_forecasts = list(
    total_gaps = ifelse(exists("municipal_forecast_gaps"), nrow(municipal_forecast_gaps), 0),
    coverage_percent = ifelse(exists("forecast_grid") && exists("available_forecasts"),
                             round(100 * nrow(available_forecasts) / nrow(forecast_grid), 1), 0)
  ),
  hourly_continuity = list(
    recent_observations = ifelse(exists("recent_hourly"), nrow(recent_hourly), 0),
    avg_daily_count = ifelse(exists("daily_counts"), round(mean(daily_counts$N), 0), 0)
  )
)

# Save detailed gaps if they exist
if(nrow(station_daily_gaps) > 0) {
  fwrite(station_daily_gaps, paste0("data/output/gaps_station_daily_", ANALYSIS_DATE, ".csv"))
  cat("Station daily gaps saved to: data/output/gaps_station_daily_", ANALYSIS_DATE, ".csv\n")
}

if(nrow(municipal_forecast_gaps) > 0) {
  fwrite(municipal_forecast_gaps, paste0("data/output/gaps_municipal_forecasts_", ANALYSIS_DATE, ".csv"))
  cat("Municipal forecast gaps saved to: data/output/gaps_municipal_forecasts_", ANALYSIS_DATE, ".csv\n")
}

# Save summary as JSON for easy parsing
jsonlite::write_json(gap_summary, paste0("data/output/gap_analysis_summary_", ANALYSIS_DATE, ".json"), 
                    pretty = TRUE, auto_unbox = TRUE)
cat("Gap analysis summary saved to: data/output/gap_analysis_summary_", ANALYSIS_DATE, ".json\n")

cat("\n=== GAP ANALYSIS COMPLETE ===\n")
cat("Analysis completed at:", format(Sys.time()), "\n")
