#!/usr/bin/env Rscript

# Test both climaemet and meteospain packages for forecast collection
# Evaluate which works better for our three key datasets

cat("=== TESTING AEMET PACKAGES ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Load required libraries
library(data.table)
library(dplyr)

# Load API keys
source("auth/keys.R")

# Load municipality data for testing
municipalities_data = fread("data/input/municipalities.csv.gz")
test_municipalities = head(municipalities_data$CUMUN, 5)
cat("Testing with", length(test_municipalities), "municipalities\n")

# Test 1: climaemet package
cat("\n=== TESTING CLIMAEMET ===\n")
tryCatch({
  library(climaemet)
  
  # Set API key for climaemet
  aemet_api_key(my_api_key, install = TRUE, overwrite = TRUE)
  
  start_time = Sys.time()
  
  # Test daily forecasts
  cat("Testing climaemet daily forecasts...\n")
  climaemet_forecasts = aemet_forecast_daily(
    x = test_municipalities,
    verbose = TRUE,
    progress = TRUE
  )
  
  end_time = Sys.time()
  climaemet_duration = as.numeric(difftime(end_time, start_time, units = "secs"))
  
  cat("✅ CLIMAEMET SUCCESS\n")
  cat("Records retrieved:", nrow(climaemet_forecasts), "\n")
  cat("Duration:", round(climaemet_duration, 2), "seconds\n")
  cat("Rate:", round(nrow(climaemet_forecasts) / climaemet_duration, 2), "records/second\n")
  
  # Test data extraction
  cat("Testing data extraction...\n")
  temp_data = aemet_forecast_tidy(climaemet_forecasts, "temperatura")
  humidity_data = aemet_forecast_tidy(climaemet_forecasts, "humedadRelativa")
  
  cat("Temperature records:", nrow(temp_data), "\n")
  cat("Humidity records:", nrow(humidity_data), "\n")
  
}, error = function(e) {
  cat("❌ CLIMAEMET FAILED:", e$message, "\n")
  climaemet_duration = NA
})

# Test 2: meteospain package  
cat("\n=== TESTING METEOSPAIN ===\n")
tryCatch({
  library(meteospain)
  
  start_time = Sys.time()
  
  # meteospain doesn't have municipality forecasts, test station data instead
  cat("Testing meteospain station data...\n")
  
  # Get current observations
  meteospain_options = aemet_options(
    api_key = my_api_key,
    resolution = 'current_day'
  )
  
  meteospain_data = get_meteo_from('aemet', meteospain_options)
  
  end_time = Sys.time()  
  meteospain_duration = as.numeric(difftime(end_time, start_time, units = "secs"))
  
  cat("✅ METEOSPAIN SUCCESS\n")
  cat("Records retrieved:", nrow(meteospain_data), "\n")
  cat("Duration:", round(meteospain_duration, 2), "seconds\n")
  cat("Rate:", round(nrow(meteospain_data) / meteospain_duration, 2), "records/second\n")
  
}, error = function(e) {
  cat("❌ METEOSPAIN FAILED:", e$message, "\n")
  meteospain_duration = NA
})

# Test 3: Our current custom approach (quick test)
cat("\n=== TESTING CURRENT CUSTOM APPROACH ===\n")
tryCatch({
  library(curl)
  library(jsonlite)
  
  start_time = Sys.time()
  
  # Test one municipality with our current approach
  h = new_handle()
  handle_setheaders(h, 'api_key' = my_api_key)
  
  municipio_code = test_municipalities[1]
  response1 = curl_fetch_memory(
    paste0('https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/', municipio_code), 
    handle = h
  )
  
  if(response1$status_code == 200) {
    response_content = fromJSON(rawToChar(response1$content))
    if("datos" %in% names(response_content)) {
      response2 = curl_fetch_memory(response_content$datos)
      if(response2$status_code == 200) {
        cat("✅ CUSTOM APPROACH SUCCESS for 1 municipality\n")
      }
    }
  }
  
  end_time = Sys.time()
  custom_duration = as.numeric(difftime(end_time, start_time, units = "secs"))
  cat("Duration for 1 municipality:", round(custom_duration, 2), "seconds\n")
  
}, error = function(e) {
  cat("❌ CUSTOM APPROACH FAILED:", e$message, "\n")
})

cat("\n=== PACKAGE COMPARISON SUMMARY ===\n")
cat("1. climaemet: Specialized for AEMET, handles municipality forecasts\n")
cat("2. meteospain: General Spanish weather, better for station data\n") 
cat("3. custom: Full control but error-prone\n")

cat("\n=== RECOMMENDATION ===\n")
cat("For your 3 datasets:\n")
cat("1. Municipal forecasts: USE CLIMAEMET (handles forecasts best)\n")
cat("2. Station observations: USE METEOSPAIN (handles station data well)\n")  
cat("3. Historical data: KEEP CUSTOM (already working)\n")

cat("Completed at:", format(Sys.time()), "\n")
