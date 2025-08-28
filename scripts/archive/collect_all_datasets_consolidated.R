#!/usr/bin/env Rscript

# collect_all_datasets_consolidated.R
# THREE-FILE STRATEGY: Appends new data to consolidated files
# 1. daily_station_historical.csv - All station daily data
# 2. daily_municipal_extended.csv - All municipal data (forecasts + interpolations)  
# 3. hourly_station_ongoing.csv - All hourly data

rm(list=ls())
library(tidyverse)
library(lubridate)
library(data.table)

# Configuration
COLLECT_STATION_DATA = TRUE
COLLECT_MUNICIPAL_DATA = TRUE
COLLECT_HOURLY_DATA = TRUE

start_time = Sys.time()
times = list()

cat("=======================================\n")
cat("HYBRID WEATHER DATA COLLECTION SYSTEM\n")
cat("THREE-FILE CONSOLIDATED STRATEGY\n")
cat("=======================================\n")
cat("Started at:", format(start_time), "\n\n")

# Function to safely append data without duplicates
append_to_consolidated = function(new_data, output_file, key_cols) {
  if(nrow(new_data) == 0) {
    cat("No new data to append\n")
    return(NULL)
  }
  
  # Load existing data if file exists
  if(file.exists(output_file)) {
    cat("Loading existing data from", basename(output_file), "...")
    existing_data = fread(output_file)
    cat(" (", nrow(existing_data), "rows)\n")
    
    # Combine and remove duplicates
    combined_data = rbind(existing_data, new_data, fill = TRUE)
    original_rows = nrow(combined_data)
    
    # Remove duplicates based on key columns
    if(all(key_cols %in% names(combined_data))) {
      combined_data = combined_data[!duplicated(combined_data[, ..key_cols]), ]
    }
    
    duplicates_removed = original_rows - nrow(combined_data)
    new_rows_added = nrow(combined_data) - nrow(existing_data)
    
    cat("Added", new_rows_added, "new rows (", duplicates_removed, "duplicates removed)\n")
  } else {
    cat("Creating new file", basename(output_file), "\n")
    combined_data = new_data
    new_rows_added = nrow(combined_data)
  }
  
  # Sort data appropriately
  if("date" %in% names(combined_data)) {
    combined_data = combined_data[order(date)]
  } else if("fhora" %in% names(combined_data)) {
    combined_data = combined_data[order(fhora)]
  }
  
  # Save updated file
  fwrite(combined_data, output_file)
  fwrite(combined_data, paste0(output_file, ".gz"))
  
  cat("✅ Updated", basename(output_file), "- Total rows:", nrow(combined_data), "\n")
  return(combined_data)
}

if(COLLECT_STATION_DATA) {
  cat("=== DATASET 1: DAILY STATION DATA ===\n")
  cat("Collecting daily means, minimums, and maximums by weather station\n")
  
  dataset1_start = Sys.time()
  
  tryCatch({
    # Collect new station data (temporary file)
    source("code/get_station_daily_hybrid.R")
    
    # Load the new data from temporary file
    temp_files = list.files("data/output", pattern = "station_daily_data_.*\\.csv$", full.names = TRUE)
    if(length(temp_files) > 0) {
      latest_temp = temp_files[which.max(file.mtime(temp_files))]
      new_station_data = fread(latest_temp)
      
      # Standardize column names
      if("indicativo" %in% names(new_station_data)) {
        new_station_data$idema = new_station_data$indicativo
      }
      if("fecha" %in% names(new_station_data)) {
        new_station_data$date = as.Date(new_station_data$fecha)
      }
      
      # Append to consolidated file
      append_to_consolidated(new_station_data, "data/output/daily_station_historical.csv", c("idema", "date"))
      
      # Clean up temporary file
      file.remove(latest_temp)
      cat("Cleaned up temporary file:", basename(latest_temp), "\n")
    }
    
    dataset1_end = Sys.time()
    times$station_daily = as.numeric(difftime(dataset1_end, dataset1_start, units = "mins"))
    cat("✅ Dataset 1 completed in", round(times$station_daily, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 1 failed:", e$message, "\n\n")
    times$station_daily = NA
  })
}

if(COLLECT_MUNICIPAL_DATA) {
  cat("=== DATASET 2: MUNICIPAL FORECASTS ===\n")
  cat("Collecting municipal data with 7-day forecasts using climaemet\n")
  
  dataset2_start = Sys.time()
  
  tryCatch({
    # Collect new municipal data (temporary file)
    source("code/get_forecast_data_hybrid.R")
    
    # Load the new data from temporary file
    temp_files = list.files("data/output", pattern = "municipal_forecasts_.*\\.csv$", full.names = TRUE)
    if(length(temp_files) > 0) {
      latest_temp = temp_files[which.max(file.mtime(temp_files))]
      new_municipal_data = fread(latest_temp)
      
      # Standardize date column
      if("fecha" %in% names(new_municipal_data)) {
        new_municipal_data$date = as.Date(new_municipal_data$fecha)
      }
      
      # Append to consolidated file
      append_to_consolidated(new_municipal_data, "data/output/daily_municipal_extended.csv", c("id", "fecha"))
      
      # Clean up temporary file
      file.remove(latest_temp)
      cat("Cleaned up temporary file:", basename(latest_temp), "\n")
    }
    
    dataset2_end = Sys.time()
    times$municipal_forecasts = as.numeric(difftime(dataset2_end, dataset2_start, units = "mins"))
    cat("✅ Dataset 2 completed in", round(times$municipal_forecasts, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 2 failed:", e$message, "\n\n")
    times$municipal_forecasts = NA
  })
}

if(COLLECT_HOURLY_DATA) {
  cat("=== DATASET 3: HOURLY DATA ===\n")
  cat("Collecting hourly data for building history\n")
  
  dataset3_start = Sys.time()
  
  tryCatch({
    # Collect new hourly data (temporary file)
    source("code/get_latest_data.R")
    
    # Load the new data from temporary file
    temp_files = list.files("data/output", pattern = "latest_weather_.*\\.csv$", full.names = TRUE)
    if(length(temp_files) > 0) {
      latest_temp = temp_files[which.max(file.mtime(temp_files))]
      new_hourly_data = fread(latest_temp)
      
      # Append to consolidated file
      append_to_consolidated(new_hourly_data, "data/output/hourly_station_ongoing.csv", c("idema", "fhora"))
      
      # Clean up temporary file
      file.remove(latest_temp)
      cat("Cleaned up temporary file:", basename(latest_temp), "\n")
    }
    
    dataset3_end = Sys.time()
    times$hourly_data = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    cat("✅ Dataset 3 completed in", round(times$hourly_data, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    times$hourly_data = NA
  })
}

# === POST-COLLECTION GAP ANALYSIS AND MONITORING ===
cat("=== POST-COLLECTION ANALYSIS ===\n")

# Run gap analysis
tryCatch({
  source("code/check_data_gaps.R")
}, error = function(e) {
  cat("⚠️  Gap analysis failed:", e$message, "\n")
})

# Update README with current status
tryCatch({
  source("code/update_readme_with_summary.R")
}, error = function(e) {
  cat("⚠️  README update failed:", e$message, "\n")
})

# Final summary
end_time = Sys.time()
total_time = as.numeric(difftime(end_time, start_time, units = "mins"))

cat("========================================\n")
cat("COLLECTION SUMMARY\n")
cat("========================================\n")
cat("Total execution time:", round(total_time, 2), "minutes\n\n")

cat("Individual dataset times:\n")
if(!is.null(times$station_daily) && !is.na(times$station_daily)) {
  cat("  Dataset 1 (Station Daily):", round(times$station_daily, 2), "minutes\n")
}
if(!is.null(times$municipal_forecasts) && !is.na(times$municipal_forecasts)) {
  cat("  Dataset 2 (Municipal Forecasts):", round(times$municipal_forecasts, 2), "minutes\n")
}
if(!is.null(times$hourly_data) && !is.na(times$hourly_data)) {
  cat("  Dataset 3 (Hourly Data):", round(times$hourly_data, 2), "minutes\n")
}

cat("\nFinal consolidated datasets:\n")
final_files = c(
  "data/output/daily_station_historical.csv",
  "data/output/daily_municipal_extended.csv", 
  "data/output/hourly_station_ongoing.csv"
)

for(file in final_files) {
  if(file.exists(file)) {
    file_size = round(file.size(file) / 1024 / 1024, 2)
    rows = nrow(fread(file, nrows = 0))
    if(file.exists(file)) {
      # Get row count more efficiently
      wc_output = system(paste("wc -l", file), intern = TRUE)
      row_count = as.numeric(strsplit(wc_output, " ")[[1]][1]) - 1  # subtract header
      cat("  ✅", basename(file), "(", file_size, "MB,", format(row_count, big.mark=","), "rows )\n")
    }
  } else {
    cat("  ❌", basename(file), "(not found)\n")
  }
}

cat("\nCompleted at:", format(Sys.time()), "\n")
cat("Three-file strategy: All data consolidated, no duplicates, no fragmentation\n")
