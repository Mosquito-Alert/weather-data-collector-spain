#!/usr/bin/env Rscript

# fill_data_gaps.R
# Intelligent gap filling that avoids redundant historical collection
# Uses gap analysis to target specific missing data

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== SMART GAP FILLING SYSTEM ===\n")
cat("Started at:", format(Sys.time()), "\n\n")

# Load gap analysis results
ANALYSIS_DATE = Sys.Date()
gap_file = paste0("data/output/gap_analysis_summary_", ANALYSIS_DATE, ".json")

if(!file.exists(gap_file)) {
  cat("ERROR: Gap analysis not found. Run check_data_gaps.R first.\n")
  quit(save = "no", status = 1)
}

gap_summary = jsonlite::read_json(gap_file)
cat("Loading gap analysis from:", gap_file, "\n")

# === 1. TARGETED STATION DAILY COLLECTION ===
cat("\n1. TARGETED STATION DAILY COLLECTION\n")
cat("====================================\n")

station_gaps_file = paste0("data/output/gaps_station_daily_", ANALYSIS_DATE, ".csv")
if(file.exists(station_gaps_file)) {
  station_gaps = fread(station_gaps_file)
  station_gaps$date = as.Date(station_gaps$date)
  
  # Focus on recent gaps (last 30 days) - avoid deep historical redundancy
  recent_cutoff = ANALYSIS_DATE - 30
  recent_gaps = station_gaps[date >= recent_cutoff]
  
  cat("Total station gaps:", nrow(station_gaps), "\n")
  cat("Recent gaps (last 30 days):", nrow(recent_gaps), "\n")
  
  if(nrow(recent_gaps) > 0) {
    cat("Targeting recent gaps for collection...\n")
    
    # Create focused date range for collection
    gap_start = min(recent_gaps$date)
    gap_end = max(recent_gaps$date)
    
    cat("Gap filling date range:", as.character(gap_start), "to", as.character(gap_end), "\n")
    
    # Modify station daily script to target specific date range
    cat("Executing targeted station daily collection...\n")
    
    # Set environment variables for the collection script
    Sys.setenv(TARGET_START_DATE = as.character(gap_start))
    Sys.setenv(TARGET_END_DATE = as.character(gap_end))
    Sys.setenv(GAP_FILLING_MODE = "TRUE")
    
    tryCatch({
      source("code/get_station_daily_hybrid.R")
      cat("✅ Targeted station collection completed.\n")
    }, error = function(e) {
      cat("❌ Station collection failed:", e$message, "\n")
    })
    
  } else {
    cat("✅ No recent station gaps to fill.\n")
  }
} else {
  cat("No station gaps file found. All station data may be complete.\n")
}

# === 2. TARGETED MUNICIPAL FORECAST COLLECTION ===
cat("\n2. TARGETED MUNICIPAL FORECAST COLLECTION\n")
cat("=========================================\n")

forecast_gaps_file = paste0("data/output/gaps_municipal_forecasts_", ANALYSIS_DATE, ".csv")
if(file.exists(forecast_gaps_file)) {
  forecast_gaps = fread(forecast_gaps_file)
  forecast_gaps$fecha = as.Date(forecast_gaps$fecha)
  
  # Focus on current and future dates only
  current_and_future = forecast_gaps[fecha >= ANALYSIS_DATE]
  
  cat("Total forecast gaps:", nrow(forecast_gaps), "\n")
  cat("Current/future gaps:", nrow(current_and_future), "\n")
  
  if(nrow(current_and_future) > 0) {
    cat("Targeting current/future forecast gaps...\n")
    
    # Get unique municipalities needing forecasts
    missing_municipalities = unique(current_and_future$municipio_code)
    cat("Municipalities needing forecasts:", length(missing_municipalities), "\n")
    
    # Set targeted collection mode
    Sys.setenv(TARGET_MUNICIPALITIES = paste(missing_municipalities, collapse = ","))
    Sys.setenv(GAP_FILLING_MODE = "TRUE")
    
    tryCatch({
      source("code/get_forecast_data_hybrid.R")
      cat("✅ Targeted forecast collection completed.\n")
    }, error = function(e) {
      cat("❌ Forecast collection failed:", e$message, "\n")
    })
    
  } else {
    cat("✅ No current/future forecast gaps to fill.\n")
  }
} else {
  cat("No forecast gaps file found. All forecasts may be complete.\n")
}

# === 3. SMART HISTORICAL COLLECTION (AVOID REDUNDANCY) ===
cat("\n3. SMART HISTORICAL COLLECTION\n")
cat("==============================\n")

# Check if we need historical data that we've never collected
historical_marker_file = "data/output/historical_collection_markers.csv"

# Create tracking system for historical collection
if(!file.exists(historical_marker_file)) {
  cat("Creating historical collection tracking system...\n")
  
  # Initialize with empty tracking
  historical_markers = data.table(
    collection_type = character(),
    date_collected = character(),
    date_range_start = character(),
    date_range_end = character(),
    records_collected = integer()
  )
  
  fwrite(historical_markers, historical_marker_file)
} else {
  historical_markers = fread(historical_marker_file)
}

cat("Historical collection markers loaded.\n")

# Check for deep historical gaps (only if we haven't collected recently)
deep_historical_cutoff = as.Date("2023-01-01")
last_historical_collection = historical_markers[collection_type == "station_historical"]

if(nrow(last_historical_collection) == 0 || 
   max(as.Date(last_historical_collection$date_collected)) < (ANALYSIS_DATE - 30)) {
  
  cat("Historical collection needed (no recent historical run).\n")
  
  # Check for very old station gaps
  if(exists("station_gaps")) {
    old_gaps = station_gaps[date < deep_historical_cutoff]
    
    if(nrow(old_gaps) > 10000) {  # Only if significant gaps
      cat("Large historical gaps detected. Running historical collection...\n")
      
      tryCatch({
        source("code/get_historical_data.R")
        
        # Record this collection
        new_marker = data.table(
          collection_type = "station_historical",
          date_collected = as.character(ANALYSIS_DATE),
          date_range_start = as.character(deep_historical_cutoff),
          date_range_end = as.character(ANALYSIS_DATE - 30),
          records_collected = 0  # Will be updated by collection script
        )
        
        historical_markers = rbind(historical_markers, new_marker, fill = TRUE)
        fwrite(historical_markers, historical_marker_file)
        
        cat("✅ Historical collection completed and recorded.\n")
      }, error = function(e) {
        cat("❌ Historical collection failed:", e$message, "\n")
      })
    } else {
      cat("✅ Historical gaps are manageable, skipping deep collection.\n")
    }
  }
} else {
  cat("✅ Recent historical collection detected, skipping redundant collection.\n")
}

# === 4. UPDATE AGGREGATIONS ===
cat("\n4. UPDATING AGGREGATIONS\n")
cat("========================\n")

cat("Updating station daily aggregation...\n")
tryCatch({
  source("code/aggregate_daily_station_data_hybrid.R")
  cat("✅ Station aggregation updated.\n")
}, error = function(e) {
  cat("❌ Station aggregation failed:", e$message, "\n")
})

cat("Updating municipal aggregation...\n")
tryCatch({
  source("code/aggregate_municipal_data_hybrid.R")
  cat("✅ Municipal aggregation updated.\n")
}, error = function(e) {
  cat("❌ Municipal aggregation failed:", e$message, "\n")
})

# === 5. POST-FILL GAP ANALYSIS ===
cat("\n5. POST-FILL GAP ANALYSIS\n")
cat("=========================\n")

cat("Running post-fill gap analysis...\n")
tryCatch({
  source("code/check_data_gaps.R")
  cat("✅ Post-fill gap analysis completed.\n")
}, error = function(e) {
  cat("❌ Post-fill gap analysis failed:", e$message, "\n")
})

cat("\n=== GAP FILLING COMPLETE ===\n")
cat("Completed at:", format(Sys.time()), "\n")
