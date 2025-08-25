#!/usr/bin/env Rscript

# HYBRID APPROACH: Station daily data using existing working methods
# Keep using the proven approach for daily station means/min/max
# Part of the 3-dataset strategy for Spanish weather data

cat("=== DAILY STATION DATA COLLECTION ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Load required libraries
library(httr)
library(jsonlite)
library(data.table)
library(dplyr)

# Load API keys
source("auth/keys.R")

# Configuration
END_DATE = Sys.Date() - 1  # Yesterday
START_DATE = END_DATE - 7  # Last 7 days
TESTING_MODE = FALSE

cat("Collecting station data from", as.character(START_DATE), "to", as.character(END_DATE), "\n")

# Load station metadata
cat("Loading station information...\n")
stations_data = fread("data/input/station_point_municipaities_table.csv")
all_stations = stations_data$INDICATIVO
cat("Loaded", length(all_stations), "stations\n")

if(TESTING_MODE) {
  all_stations = head(all_stations, 20)
  cat("Testing mode: using", length(all_stations), "stations\n")
}

# Function to safely get daily data for a station
get_station_daily_data <- function(station_id, start_date, end_date, api_key) {
  tryCatch({
    # Format dates for API
    start_str = format(start_date, "%Y-%m-%dT00:00:00UTC")
    end_str = format(end_date, "%Y-%m-%dT23:59:59UTC")
    
    url = paste0("https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/",
                 start_str, "/fechafin/", end_str, "/estacion/", station_id)
    
    # First API call to get data URL
    response1 = GET(url, query = list(api_key = api_key))
    
    if(status_code(response1) != 200) {
      return(NULL)
    }
    
    # Get the actual data URL
    json_response = fromJSON(rawToChar(response1$content))
    data_url = json_response$datos
    
    if(is.null(data_url) || data_url == "") {
      return(NULL)
    }
    
    # Get the actual data
    response2 = GET(data_url)
    
    if(status_code(response2) != 200) {
      return(NULL)
    }
    
    # Parse JSON data
    raw_data = fromJSON(rawToChar(response2$content))
    
    if(length(raw_data) == 0) {
      return(NULL)
    }
    
    # Convert to data table and process
    daily_data = as.data.table(raw_data)
    
    # Standardize column names and select key variables
    if(nrow(daily_data) > 0) {
      daily_data[, station_id := station_id]
      daily_data[, collected_at := Sys.time()]
      
      # Convert numeric columns
      numeric_cols = c("tmed", "tmax", "tmin", "prec", "velmedia", "racha", "presMax", "presMin")
      for(col in numeric_cols) {
        if(col %in% names(daily_data)) {
          daily_data[[col]] = as.numeric(gsub(",", ".", daily_data[[col]]))
        }
      }
      
      return(daily_data)
    }
    
    return(NULL)
    
  }, error = function(e) {
    return(NULL)
  })
}

# Collect data in batches
BATCH_SIZE = 100
batches = split(all_stations, ceiling(seq_along(all_stations) / BATCH_SIZE))
total_batches = length(batches)

cat("Processing", length(all_stations), "stations in", total_batches, "batches\n\n")

all_daily_data = list()
successful_stations = 0

for(batch_num in seq_along(batches)) {
  cat("=== BATCH", batch_num, "of", total_batches, "===\n")
  current_batch = batches[[batch_num]]
  
  batch_start_time = Sys.time()
  batch_data = list()
  
  for(i in seq_along(current_batch)) {
    station_id = current_batch[i]
    
    # Show progress every 20 stations
    if(i %% 20 == 1) {
      cat("Processing station", i, "of", length(current_batch), "in batch", batch_num, ":", station_id, "\n")
    }
    
    station_data = get_station_daily_data(station_id, START_DATE, END_DATE, my_api_key)
    
    if(!is.null(station_data) && nrow(station_data) > 0) {
      batch_data[[length(batch_data) + 1]] = station_data
      successful_stations = successful_stations + 1
    }
    
    # Small delay to be respectful to API
    Sys.sleep(0.5)
  }
  
  # Combine batch data
  if(length(batch_data) > 0) {
    batch_combined = rbindlist(batch_data, fill = TRUE)
    all_daily_data[[batch_num]] = batch_combined
    
    batch_end_time = Sys.time()
    batch_duration = as.numeric(difftime(batch_end_time, batch_start_time, units = "mins"))
    
    cat("✅ Batch", batch_num, "completed\n")
    cat("Duration:", round(batch_duration, 2), "minutes\n")
    cat("Records in batch:", nrow(batch_combined), "\n")
    cat("Successful stations so far:", successful_stations, "out of", batch_num * BATCH_SIZE, "\n\n")
  } else {
    cat("❌ No data collected in batch", batch_num, "\n\n")
  }
}

# Combine all data and save
cat("=== FINAL PROCESSING ===\n")
if(length(all_daily_data) > 0) {
  final_data = rbindlist(all_daily_data, fill = TRUE)
  
  cat("Total daily records:", nrow(final_data), "\n")
  cat("Stations with data:", length(unique(final_data$station_id)), "out of", length(all_stations), "\n")
  cat("Success rate:", round(100 * length(unique(final_data$station_id)) / length(all_stations), 1), "%\n")
  
  # Save results
  dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
  
  output_file = paste0("data/output/station_daily_data_", Sys.Date(), ".csv")
  write.csv(final_data, output_file, row.names = FALSE)
  cat("Data saved to:", output_file, "\n")
  
  # Compressed version
  output_file_gz = paste0(output_file, ".gz")
  fwrite(final_data, output_file_gz)
  cat("Compressed version saved to:", output_file_gz, "\n")
  
  # Summary
  cat("\n=== SUMMARY ===\n")
  cat("Stations processed:", length(unique(final_data$station_id)), "\n")
  cat("Total station-days:", nrow(final_data), "\n")
  cat("Date range:", as.character(min(as.Date(final_data$fecha))), "to", as.character(max(as.Date(final_data$fecha))), "\n")
  
  print(head(final_data, 3))
  
} else {
  cat("❌ No data collected\n")
  quit(save = "no", status = 1)
}

cat("\nCompleted at:", format(Sys.time()), "\n")
