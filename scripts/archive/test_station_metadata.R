#!/usr/bin/env Rscript

# Test script to explore AEMET API for station metadata
# Looking for endpoints that provide station locations/municipality mappings

library(curl)
library(jsonlite)

# Load API keys
source("auth/keys.R")

# Set up curl handle with API key
h = new_handle()
handle_setopt(h, customrequest = "GET")
handle_setheaders(h, "api_key" = get_current_api_key())

cat("Testing AEMET API endpoints for station metadata...\n\n")

# Test different potential endpoints for station information
test_endpoints <- c(
  "maestro/estacion",  # Master station list
  "estaciones",        # Stations
  "estaciones/todas",  # All stations  
  "maestro/estaciones", # Master stations
  "observacion/convencional/estaciones", # Observation stations
  "valores/climatologicos/estaciones",   # Climatological stations
  "red/estaciones"     # Station network
)

for(endpoint in test_endpoints) {
  cat("Testing endpoint:", endpoint, "\n")
  
  tryCatch({
    url <- paste0('https://opendata.aemet.es/opendata/api/', endpoint)
    req = curl_fetch_memory(url, handle=h)
    
    if(req$status_code == 200) {
      response_content = fromJSON(rawToChar(req$content))
      cat("  ✅ SUCCESS - Status:", req$status_code, "\n")
      
      if("datos" %in% names(response_content)) {
        cat("  📊 Has 'datos' field - fetching actual data...\n")
        
        # Get the actual data
        data_req = curl_fetch_memory(response_content$datos)
        if(data_req$status_code == 200) {
          station_data = fromJSON(rawToChar(data_req$content))
          cat("  📈 Data retrieved successfully\n")
          cat("  📋 Number of records:", length(station_data), "\n")
          
          if(length(station_data) > 0) {
            # Show structure of first record
            cat("  🔍 First record structure:\n")
            str(station_data[[1]])
            cat("  📍 Available fields:", paste(names(station_data[[1]]), collapse = ", "), "\n")
            
            # Look for municipality or location fields
            location_fields <- names(station_data[[1]])[grepl("munic|provincia|ciudad|localidad|ubicacion|lon|lat", names(station_data[[1]]), ignore.case = TRUE)]
            if(length(location_fields) > 0) {
              cat("  🎯 FOUND LOCATION FIELDS:", paste(location_fields, collapse = ", "), "\n")
            }
          }
        } else {
          cat("  ❌ Failed to fetch data - Status:", data_req$status_code, "\n")
        }
      } else {
        cat("  📋 Direct response fields:", paste(names(response_content), collapse = ", "), "\n")
      }
      
    } else {
      cat("  ❌ FAILED - Status:", req$status_code, "\n")
    }
    
  }, error = function(e) {
    cat("  ❌ ERROR:", e$message, "\n")
  })
  
  cat("\n")
}

cat("Station metadata exploration complete.\n")
