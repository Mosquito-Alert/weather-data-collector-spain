# get_historical_data.R
# ----------------------
# Purpose: Download and update historical daily weather data for Spain from the AEMET OpenData API.
#
# This script fetches historical daily climatological data using the 7 core variables
# that are compatible across current observations, historical data, and forecast endpoints.
#
# Core Variables (Standardized):
#   - ta: Air temperature (°C) - from tmed
#   - tamax: Maximum temperature (°C) - from tmax 
#   - tamin: Minimum temperature (°C) - from tmin
#   - hr: Relative humidity (%) - from hrMedia
#   - prec: Precipitation (mm) - from prec
#   - vv: Wind speed (km/h) - from velmedia
#   - pres: Atmospheric pressure (hPa) - from presMax
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

# If you want to prevent concurrent runs of this script, set PREVENT_CONCURRENT_RUNS to TRUE.
PREVENT_CONCURRENT_RUNS = FALSE

if(PREVENT_CONCURRENT_RUNS) {
  # Prevent concurrent runs by creating a lockfile
  # Lockfile management
  lockfile <- "tmp/get_historical_data.lock"
  # Check if lockfile exists
  if (file.exists(lockfile)) {
    cat("Another run is in progress. Exiting.\n")
    quit(save = "no", status = 0)
  }
  # Create a temporary directory and lockfile
  dir.create("tmp", showWarnings = FALSE)
  file.create(lockfile)
  # Ensure lockfile is removed on exit
  on.exit(unlink(lockfile), add = TRUE)
}

# Load API keys
source("auth/keys.R")

# Set locale to UTF-8 for proper encoding handling
Sys.setlocale("LC_ALL", "en_US.UTF-8")

# SETTING DATES ####
# Set the start date for historical data collection
start_date = as_date("2013-07-01")

# Set up curl handle with API key for authentication
h <- new_handle()
handle_setheaders(h, 'api_key' = my_api_key)

# Generate sequence of all dates to check (from start_date to 4 days before today)
all_dates = seq.Date(from = start_date, to=today()-4, by = "day")

# Load existing historical weather data
if(file.exists("data/output/daily_station_historical.csv.gz")){
stored_weather_daily = fread("data/output/daily_station_historical.csv.gz")
} else{stored_weather_daily = NULL}


# Reverse date order (latest first)
all_dates = rev(all_dates)

# Identify which dates are missing from the local dataset
if(!is.null(stored_weather_daily)){
  these_dates = all_dates[which(!all_dates %in% unique(stored_weather_daily$date))]
} else{
  these_dates = all_dates
}

# Set chunk size for API requests (to avoid rate limits)
chunksize = 20

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
        
        wurl = fromJSON(rawToChar(req$content))$datos
        
        req = curl_fetch_memory(wurl)
        this_string = rawToChar(req$content)
        
        # Set encoding to handle Spanish characters properly
        Encoding(this_string) = "latin1"
        
        # Parse JSON and standardize variable names to match current observations
        wdia  = fromJSON(this_string) %>% 
          as_tibble() %>% 
          select(
            date = fecha, 
            indicativo, 
            ta = tmed,        # Mean temperature -> ta
            tamax = tmax,     # Maximum temperature -> tamax  
            tamin = tmin,     # Minimum temperature -> tamin
            hr = hrMedia,     # Mean humidity -> hr
            prec = prec,      # Precipitation -> prec
            vv = velmedia,    # Wind speed -> vv
            pres = presMax    # Pressure (using max) -> pres
          ) %>% 
          mutate(
            date = as_date(date),
            ta = as.numeric(str_replace(ta, ",", ".")),
            tamax = as.numeric(str_replace(tamax, ",", ".")),
            tamin = as.numeric(str_replace(tamin, ",", ".")),
            hr = as.numeric(str_replace(hr, ",", ".")),
            prec = as.numeric(str_replace(prec, ",", ".")),
            vv = as.numeric(str_replace(vv, ",", ".")),
            pres = as.numeric(str_replace(pres, ",", "."))
          ) %>% 
          as.data.table()
        return(wdia)
        
      },
      error = function(e){ 
        # (Optional)
        # Do this if an error is caught...
        print(e)
        Sys.sleep(50)
        return(NULL)
      },
      warning = function(w){
        print(w)
        
        # (Optional)
        # Do this if a warning is caught...
        return(NULL)
      },
      finally = {
        # (Optional)
        # Do this at the end before quitting the tryCatch structure...
      }
    )
    
  }))
  
  if(!is.null(stored_weather_daily)){
  weather_daily = rbindlist(list(weather_daily, stored_weather_daily))
  }
  
  print("writing chunk")
  
  fwrite(weather_daily, "data/output/daily_station_historical.csv.gz")
  
  print("pausing 30 seconds")
  Sys.sleep(30)
  
})

} else{
  
  print("Up to date - no historical data downloaded")
}

