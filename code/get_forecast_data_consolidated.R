#!/usr/bin/env Rscript

# get_forecast_data_consolidated.R
# Collects municipal forecasts and appends to daily_municipal_extended.csv
# Implements priority logic: actual station data > forecast data

rm(list=ls())

library(climaemet)
library(tidyverse)
library(lubridate)
library(data.table)

cat("=== MUNICIPAL FORECAST DATA COLLECTION (CONSOLIDATED) ===\n")

# Load API key
if(file.exists("auth/keys.R")) {
  source("auth/keys.R")
  aemet_api_key(api_key)
  cat("✅ AEMET API key loaded\n")
} else {
  cat("❌ API key file not found. Please check auth/keys.R\n")
  quit(save="no", status=1)
}

# Municipal codes
municipal_codes = c(
  "28001", "08019", "46250", "41091", "29067", "03014", "30030", "39075",
  "17079", "15030", "32054", "27028", "36057", "12040", "02003", "16078",
  "09059", "40194", "37274", "05019", "06015", "10037", "42173", "22125"
)

collected_forecasts = data.table()
successful_count = 0
failed_count = 0

cat("Starting municipal forecast collection for", length(municipal_codes), "municipalities...\n")
start_time = Sys.time()

for(i in seq_along(municipal_codes)) {
  codigo = municipal_codes[i]
  
  tryCatch({
    cat("Processing municipality", i, "of", length(municipal_codes), ":", codigo, "\n")
    
    # Get forecast data using climaemet
    forecast_data = aemet_forecast_municipality(codigo)
    
    if(!is.null(forecast_data) && nrow(forecast_data) > 0) {
      # Clean and standardize the data
      forecast_clean = forecast_data %>%
        mutate(
          municipio_id = codigo,
          municipio_nombre = nombre,
          provincia = provincia,
          elaborado = elaborado,
          fecha = as.Date(fecha),
          temp_max = as.numeric(temperatura_maxima),
          temp_min = as.numeric(temperatura_minima), 
          temp_avg = round((temp_max + temp_min) / 2, 1),
          humid_max = as.numeric(humedad_maxima),
          humid_min = as.numeric(humedad_minima),
          wind_speed = as.numeric(velocidad_viento),
          collected_at = Sys.time(),
          data_source = "forecast"
        ) %>%
        select(municipio_id, municipio_nombre, provincia, elaborado, fecha, 
               temp_max, temp_min, temp_avg, humid_max, humid_min, wind_speed, 
               collected_at, data_source)
      
      collected_forecasts = rbind(collected_forecasts, forecast_clean, fill = TRUE)
      successful_count = successful_count + 1
      
      cat("  ✅ Collected", nrow(forecast_clean), "forecast records\n")
    } else {
      cat("  ⚠️  No data returned\n")
      failed_count = failed_count + 1
    }
    
    # Rate limiting
    if(i %% 10 == 0) {
      cat("  Completed", i, "/", length(municipal_codes), "municipalities\n")
      Sys.sleep(2)
    } else {
      Sys.sleep(0.5)
    }
    
  }, error = function(e) {
    cat("  ❌ Error:", e$message, "\n")
    failed_count = failed_count + 1
  })
}

end_time = Sys.time()
collection_time = as.numeric(difftime(end_time, start_time, units = "mins"))

cat("\n=== COLLECTION SUMMARY ===\n")
cat("Successful municipalities:", successful_count, "\n")
cat("Failed municipalities:", failed_count, "\n")
cat("Success rate:", round(100 * successful_count / length(municipal_codes), 1), "%\n")
cat("Total forecast records:", nrow(collected_forecasts), "\n")
cat("Collection time:", round(collection_time, 2), "minutes\n")

if(nrow(collected_forecasts) > 0) {
  cat("Date range:", as.character(min(collected_forecasts$fecha)), "to", 
      as.character(max(collected_forecasts$fecha)), "\n")
  
  # ====================================================================
  # APPEND TO CONSOLIDATED FILE WITH PRIORITY LOGIC
  # ====================================================================
  cat("\n=== APPENDING TO CONSOLIDATED FILE ===\n")
  
  consolidated_file = "data/output/daily_municipal_extended.csv"
  
  # Load existing data if it exists
  existing_data = data.table()
  if(file.exists(consolidated_file)) {
    cat("Loading existing municipal data...\n")
    existing_data = fread(consolidated_file)
    existing_data$fecha = as.Date(existing_data$fecha)
    cat("Existing records:", nrow(existing_data), "\n")
  }
  
  # Standardize municipio column name for new forecasts
  collected_forecasts$municipio = collected_forecasts$municipio_id
  
  # Combine with existing data
  all_data = rbind(existing_data, collected_forecasts, fill = TRUE)
  
  # Apply priority logic: station_aggregated > forecast
  # Remove duplicate date-municipality combinations, keeping highest priority
  priority_order = c("station_aggregated", "forecast")
  all_data$priority = match(all_data$data_source, priority_order)
  all_data = all_data[order(fecha, municipio, priority)]
  final_data = all_data[!duplicated(paste(fecha, municipio))]
  
  # Save consolidated file
  fwrite(final_data, consolidated_file)
  
  cat("✅ Consolidated file updated:", consolidated_file, "\n")
  cat("Total records in consolidated file:", nrow(final_data), "\n")
  
  # Summary of data sources in final file
  source_summary = final_data[, .N, by = data_source]
  cat("Data source composition:\n")
  print(source_summary)
  
  file_size_mb = round(file.size(consolidated_file) / 1024 / 1024, 2)
  cat("File size:", file_size_mb, "MB\n")
  
} else {
  cat("⚠️  No forecast data collected\n")
}

cat("\nMunicipal forecast collection completed.\n")
