#!/usr/bin/env Rscript

# Check what station metadata we can extract from existing observation data
library(curl)
library(jsonlite)
library(dplyr)

# Load API keys
source("auth/keys.R")

# Set up curl handle with API key
h = new_handle()
handle_setopt(h, customrequest = "GET")
handle_setheaders(h, "api_key" = get_current_api_key())

cat("Checking station metadata from current observation data...\n\n")

# Get current observations (this works)
req = curl_fetch_memory('https://opendata.aemet.es/opendata/api/observacion/convencional/todas', handle=h)

if(req$status_code == 200) {
  response_content = fromJSON(rawToChar(req$content))
  cat("✅ Successfully got observations data URL\n")
  
  # Get the actual data
  data_req = curl_fetch_memory(response_content$datos)
  if(data_req$status_code == 200) {
    # Handle encoding issues by setting UTF-8
    raw_content <- rawToChar(data_req$content)
    Encoding(raw_content) <- "UTF-8"
    station_data = fromJSON(raw_content)
    cat("✅ Successfully retrieved station observation data\n")
    cat("📊 Number of stations:", length(station_data), "\n\n")
    
    if(length(station_data) > 0) {
      # Convert to data frame for easier analysis
      df <- bind_rows(station_data)
      
      cat("🔍 Available fields in observation data:\n")
      print(names(df))
      cat("\n")
      
      # Look for location-related fields
      location_fields <- names(df)[grepl("ubi|provincia|munic|ciudad|localidad|lon|lat|alt", names(df), ignore.case = TRUE)]
      cat("📍 Location-related fields found:", paste(location_fields, collapse = ", "), "\n\n")
      
      # Show sample of station identifiers and any location info
      station_info <- df %>% 
        select(any_of(c("idema", "ubi", "provincia", names(df)[grepl("lon|lat|alt", names(df), ignore.case = TRUE)]))) %>%
        distinct() %>%
        head(10)
      
      cat("📋 Sample station information:\n")
      print(station_info)
      
      # Check unique provinces if available
      if("provincia" %in% names(df)) {
        cat("\n🗺️ Available provinces:\n")
        print(sort(unique(df$provincia)))
      }
      
      cat("\n🏢 Total unique stations:", length(unique(df$idema)), "\n")
      
    } else {
      cat("❌ No station data found\n")
    }
  } else {
    cat("❌ Failed to fetch observation data - Status:", data_req$status_code, "\n")
  }
} else {
  cat("❌ Failed to get observations URL - Status:", req$status_code, "\n")
}

# Also check if we can get more detailed station info by trying some other endpoints
cat("\n", rep("=", 50), "\n", sep="")
cat("Trying alternative station info endpoints...\n\n")

alternative_endpoints <- c(
  "maestro/estacion/todas",
  "inventario/estaciones", 
  "inventario/climatologico/estaciones",
  "inventario/observacion/estaciones",
  "maestro/inventario/estaciones"
)

for(endpoint in alternative_endpoints) {
  cat("Testing:", endpoint, "\n")
  
  tryCatch({
    url <- paste0('https://opendata.aemet.es/opendata/api/', endpoint)
    req = curl_fetch_memory(url, handle=h)
    
    if(req$status_code == 200) {
      cat("  ✅ SUCCESS!\n")
      response_content = fromJSON(rawToChar(req$content))
      
      if("datos" %in% names(response_content)) {
        cat("  📊 Has data URL - attempting to fetch...\n")
        data_req = curl_fetch_memory(response_content$datos)
        if(data_req$status_code == 200) {
          station_meta = fromJSON(rawToChar(data_req$content))
          cat("  📈 Retrieved", length(station_meta), "records\n")
          
          if(length(station_meta) > 0) {
            cat("  🔍 Fields:", paste(names(station_meta[[1]]), collapse = ", "), "\n")
          }
        }
      }
    } else {
      cat("  ❌ Status:", req$status_code, "\n")
    }
    
  }, error = function(e) {
    cat("  ❌ ERROR:", e$message, "\n")
  })
  
  cat("\n")
}
