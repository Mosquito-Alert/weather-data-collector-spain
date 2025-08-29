# get_historical_data.R
# ----------------------
# Purpose: Download and update historical daily weather data for Spain from the AEMET OpenData API.
#
# This script fetches historical daily climatological data keeping original AEMET variable names
# for data integrity and to avoid mixing different API formats.
#
# Original AEMET Variables (preserved):
#   - fecha: Date
#   - indicativo: Station ID
#   - tmed: Mean temperature (°C)
#   - tmax: Maximum temperature (°C) 
#   - tmin: Minimum temperature (°C)
#   - hrMedia: Relative humidity (%)
#   - prec: Precipitation (mm)
#   - velmedia: Wind speed (km/h)
#   - presMax: Atmospheric pressure (hPa)
#
# Concurrency Control:
#   - Set PREVENT_CONCURRENT_RUNS = TRUE to enable lockfile-based run prevention
#   - Set PREVENT_CONCURRENT_RUNS = FALSE (default) to allow multiple concurrent runs
#
# Main Steps:
#   1. Load dependencies and API key.
#   2. Determine which dates are missing from the local dataset.
#   3. Download missing data in chunks, handling API rate limits and errors.
#   4. Append new data to the historical dataset.
#
# Usage:
#   - Requires a valid API key in 'auth/keys.R' as 'my_api_key'.
#   - Run as an R script. Output is written to 'data/output/daily_station_historical.csv.gz'.
#
# Dependencies: tidyverse, lubridate, data.table, curl, jsonlite
#
# Author: John Palmer
# Date: 2025-08-22 (Updated for 7-variable standardization)

# Title ####
# For downloading and preparing historical weather data. 

rm(list=ls())

####Dependencies####
library(tidyverse)
library(lubridate)
library(data.table)
library(curl)
library(jsonlite)

# Set output data file path
output_data_file_path = "data/output/daily_station_historical.csv.gz"


# Load API keys
source("auth/keys.R")

# Set locale to UTF-8 for proper encoding handling
Sys.setlocale("LC_ALL", "en_US.UTF-8")

# SETTING DATES ####
# Set the start date for historical data collection
start_date = as_date("2013-07-01")

# Set up curl handle with API key for authentication and increased timeout
h <- new_handle()
handle_setheaders(h, 'api_key' = get_current_api_key())
handle_setopt(h, timeout = 60, connecttimeout = 30)  # Increase timeout values

# Generate sequence of all dates to check (from start_date to 4 days before today)
all_dates = seq.Date(from = start_date, to=today()-4, by = "day")

# Load existing historical weather data
if(file.exists(output_data_file_path)){
stored_weather_daily = fread(output_data_file_path)
} else{stored_weather_daily = NULL}


# Reverse date order (latest first)
all_dates = rev(all_dates)

# Identify which dates are missing from the local dataset
if(!is.null(stored_weather_daily)){
  these_dates = all_dates[which(!all_dates %in% unique(stored_weather_daily$fecha))]
} else{
  these_dates = all_dates
}

# Set chunk size for API requests (reduced to avoid rate limits and timeouts)
chunksize = 5

# Main download loop: only run if there are missing dates
if(length(these_dates) > 0){

lapply(seq(1, length(these_dates), chunksize), function(j){
  
  this_chunk = these_dates[j:min(length(these_dates), (j+(chunksize-1)))]
  
  weather_daily = rbindlist(lapply(seq_along(this_chunk), function(i){
    
    start_date = this_chunk[i]
    print(start_date)
    
    tryCatch(
      expr = {
        # Request historical daily climatological data for specific date
        req = curl_fetch_memory(paste0('https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/', start_date, 'T00%3A00%3A00UTC/fechafin/', start_date, 'T23%3A59%3A59UTC/todasestaciones'), handle=h)
        
        if(req$status_code == 429) {
          cat("Rate limit - rotating key...\n")
          rotate_api_key()
          handle_setheaders(h, 'api_key' = get_current_api_key())
          Sys.sleep(3)
          req = curl_fetch_memory(paste0('https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/', start_date, 'T00%3A00%3A00UTC/fechafin/', start_date, 'T23%3A59%3A59UTC/todasestaciones'), handle=h)
          
        }
        
        if(req$status_code != 200) {
          cat("API request failed:", req$status_code, "\n")
          return(NULL)
        }
        
          
        
        wurl = fromJSON(rawToChar(req$content))$datos
        
        req = curl_fetch_memory(wurl)
        this_string = rawToChar(req$content)
        
        # Set encoding to handle Spanish characters properly
        Encoding(this_string) = "latin1"
        
        # Parse JSON keeping original AEMET variable names
        wdia  = fromJSON(this_string) %>% 
          as_tibble() %>% 
          select(
            fecha, 
            indicativo, 
            tmed,        # Mean temperature (original name)
            tmax,        # Maximum temperature (original name)
            tmin,        # Minimum temperature (original name)
            hrMedia,     # Mean humidity (original name)
            hrMax,       # Max humidity (original name)
            hrMin,       # Min humidity (original name)
            prec,        # Precipitation (original name)
            velmedia,    # Wind speed (original name)
            presMax      # Pressure (original name)
          ) %>% 
          mutate(
            fecha = as_date(fecha),
            tmed = as.numeric(str_replace(tmed, ",", ".")),
            tmax = as.numeric(str_replace(tmax, ",", ".")),
            tmin = as.numeric(str_replace(tmin, ",", ".")),
            hrMedia = as.numeric(str_replace(hrMedia, ",", ".")),
            hrMax = as.numeric(str_replace(hrMax, ",", ".")),
            hrMin = as.numeric(str_replace(hrMin, ",", ".")),
            
                        # Handle precipitation more carefully - it often contains "Ip" for trace amounts
            prec = case_when(
              is.na(prec) ~ NA_real_,
              str_detect(prec, "Ip|ip") ~ 0.1,  # Trace precipitation = 0.1mm
              prec == "" ~ NA_real_,
              TRUE ~ suppressWarnings(as.numeric(str_replace(prec, ",", ".")))
            ),
            velmedia = as.numeric(str_replace(velmedia, ",", ".")),
            presMax = as.numeric(str_replace(presMax, ",", "."))
          ) %>% 
          as.data.table()
        return(wdia)
        
      },
      error = function(e){ 
        cat("ERROR on date", as.character(start_date), ":", e$message, "\n")
        rotate_api_key()
        handle_setheaders(h, 'api_key' = get_current_api_key())
        Sys.sleep(3)
        return(NULL)
      },
      warning = function(w){
        cat("WARNING on date", as.character(start_date), ":", w$message, "\n") 
        return(NULL)
      },
      finally = {
        # (Optional)
        # Do this at the end before quitting the tryCatch structure...
      }
    )
    
  }))
  
  print(paste0("Just grabbed ", nrow(weather_daily), " new records"))
  
  if(file.exists(output_data_file_path)){
    stored_weather_daily = fread(output_data_file_path)
    
    print(paste0("We already had ", nrow(stored_weather_daily), " records stored"))
    
    weather_daily = rbindlist(list(weather_daily, stored_weather_daily))
  } 
  
   print(paste0("writing chunk with ", nrow(weather_daily), " records"))
   
   fwrite(weather_daily, output_data_file_path)
   
#   print("pausing 60 seconds")
#   Sys.sleep(60)  # Increased pause between chunks
   
 })
 
} else{
  
  print("Up to date - no historical data downloaded")
}

