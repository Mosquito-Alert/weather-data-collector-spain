#!/usr/bin/env Rscript

# High-performance forecast data collection with SSL fixes and optimizations
library(jsonlite)
library(curl)
library(dplyr)
library(data.table)
library(lubridate)
library(parallel)

# Load API keys
source("auth/keys.R")

# Configuration
TESTING_MODE = FALSE
N_TEST_MUNICIPALITIES = 2
BATCH_SIZE = 100  # Process in batches
MAX_RETRIES = 3
BASE_DELAY = 2    # Reduced from 15 seconds
MAX_WORKERS = 4   # Parallel processing

cat("=== AEMET FORECAST DATA COLLECTION (OPTIMIZED) ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Enhanced function with SSL fixes and better error handling
get_municipality_forecast_v3 = function(municipio_code, municipio_name = NULL, retry_count = 0) {
  tryCatch({
    # Configure curl handle with SSL and connection settings
    h = new_handle()
    handle_setheaders(h, 'api_key' = get_current_api_key())
    
    # SSL and connection optimizations
    handle_setopt(h,
      ssl_verifypeer = 1L,
      ssl_verifyhost = 2L,
      timeout = 30L,           # 30 second timeout
      connecttimeout = 10L,    # 10 second connection timeout
      followlocation = 1L,
      maxredirs = 5L,
      useragent = "R-forecast-collector/1.0",
      http_version = 2L        # Use HTTP/2 if available
    )
    
    # Request forecast data URL with retry logic
    response1 = NULL
    for(attempt in 1:MAX_RETRIES) {
      tryCatch({
        response1 = curl_fetch_memory(
          paste0('https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/', municipio_code), 
          handle = h
        )
        break  # Success, exit retry loop
      }, error = function(e) {
        if(attempt == MAX_RETRIES) {
          cat("✗ ERROR after", MAX_RETRIES, "attempts:", e$message, "\n")
          return(NULL)
        }
        cat("Retry", attempt, "for", municipio_code, "after error:", e$message, "\n")
        Sys.sleep(attempt)  # Progressive backoff
      })
    }
    
    if(is.null(response1)) return(NULL)
    
    # Handle rate limiting
    if(response1$status_code == 429) {
      cat("Rate limit - rotating key...\n")
      rotate_api_key()
      handle_setheaders(h, 'api_key' = get_current_api_key())
      Sys.sleep(5)
      return(get_municipality_forecast_v3(municipio_code, municipio_name, retry_count + 1))
    }
    
    if(response1$status_code != 200) {
      cat("API request failed:", response1$status_code, "for", municipio_code, "\n")
      return(NULL)
    }
    
    # Parse response to get data URL
    response_content = fromJSON(rawToChar(response1$content))
    
    if(!"datos" %in% names(response_content)) {
      cat("No data URL in response for", municipio_code, "\n")
      return(NULL)
    }
    
    # Fetch actual forecast data with retry logic
    Sys.sleep(0.5)  # Reduced delay
    response2 = NULL
    for(attempt in 1:MAX_RETRIES) {
      tryCatch({
        response2 = curl_fetch_memory(response_content$datos, handle = h)
        break
      }, error = function(e) {
        if(attempt == MAX_RETRIES) {
          cat("✗ Data fetch ERROR after", MAX_RETRIES, "attempts for", municipio_code, ":", e$message, "\n")
          return(NULL)
        }
        Sys.sleep(attempt)
      })
    }
    
    if(is.null(response2)) return(NULL)
    
    if(response2$status_code != 200) {
      cat("Data request failed:", response2$status_code, "for", municipio_code, "\n")
      return(NULL)
    }
    
    # Parse forecast data
    this_string = rawToChar(response2$content)
    Encoding(this_string) = "latin1"
    forecast_data = fromJSON(this_string)
    
    # Robust data extraction with error checking
    if(!"prediccion" %in% names(forecast_data) || 
       !"dia" %in% names(forecast_data$prediccion)) {
      cat("Invalid forecast structure for", municipio_code, "\n")
      return(NULL)
    }
    
    wdia = forecast_data$prediccion$dia
    
    if(length(wdia) == 0 || is.null(wdia[[1]])) {
      cat("No forecast days for", municipio_code, "\n")
      return(NULL)
    }
    
    # Extract vectors for all 7 days with error checking
    fechas = tryCatch({
      as.Date(wdia[[1]]$fecha)
    }, error = function(e) {
      cat("Date parsing error for", municipio_code, "\n")
      return(NULL)
    })
    
    if(is.null(fechas)) return(NULL)
    
    temp_max = if("temperatura" %in% names(wdia[[1]]) && "maxima" %in% names(wdia[[1]]$temperatura)) {
      wdia[[1]]$temperatura$maxima
    } else rep(NA, length(fechas))
    
    temp_min = if("temperatura" %in% names(wdia[[1]]) && "minima" %in% names(wdia[[1]]$temperatura)) {
      wdia[[1]]$temperatura$minima
    } else rep(NA, length(fechas))
    
    temp_avg = rowMeans(cbind(temp_max, temp_min), na.rm = TRUE)
    
    # Extract humidity with safe checking
    humid_max = if("humedadRelativa" %in% names(wdia[[1]]) && 
                    "maxima" %in% names(wdia[[1]]$humedadRelativa)) {
      wdia[[1]]$humedadRelativa$maxima
    } else rep(NA, length(fechas))
    
    humid_min = if("humedadRelativa" %in% names(wdia[[1]]) && 
                    "minima" %in% names(wdia[[1]]$humedadRelativa)) {
      wdia[[1]]$humedadRelativa$minima
    } else rep(NA, length(fechas))
    
    # Wind data with safe extraction
    wind_speed = if("viento" %in% names(wdia[[1]])) {
      tryCatch({
        unlist(lapply(wdia[[1]]$viento, function(x) {
          if(is.list(x) && "velocidad" %in% names(x)) {
            mean(x$velocidad, na.rm = TRUE)
          } else NA
        }))
      }, error = function(e) rep(NA, length(fechas)))
    } else rep(NA, length(fechas))
    
    # Create result data frame
    result = data.frame(
      municipio_id = municipio_code,
      municipio_nombre = forecast_data$nombre,
      provincia = forecast_data$provincia,
      elaborado = forecast_data$elaborado,
      fecha = fechas,
      temp_max = temp_max,
      temp_min = temp_min, 
      temp_avg = temp_avg,
      humid_max = humid_max,
      humid_min = humid_min,
      wind_speed = wind_speed,
      stringsAsFactors = FALSE
    )
    
    cat("✓ SUCCESS:", municipio_code, "- extracted", length(fechas), "days\n")
    return(result)
    
  }, error = function(e) {
    cat("✗ ERROR for", municipio_code, ":", e$message, "\n")
    return(NULL)
  })
}

# Batch processing function
process_municipality_batch = function(municipality_batch, batch_num, total_batches) {
  cat("=== PROCESSING BATCH", batch_num, "of", total_batches, "===\n")
  cat("Municipalities in batch:", length(municipality_batch), "\n")
  
  batch_results = list()
  successful_in_batch = 0
  
  for(i in seq_along(municipality_batch)) {
    code = municipality_batch[i]
    city = names(municipality_batch)[i]
    
    cat("Batch", batch_num, "- Municipality", i, "of", length(municipality_batch), ":", city, "(", code, ")\n")
    
    # Adaptive delay based on previous success rate
    if(i > 1) {
      delay = BASE_DELAY
      if(successful_in_batch / i < 0.8) {  # If success rate < 80%, increase delay
        delay = BASE_DELAY * 2
        cat("Low success rate, increasing delay to", delay, "seconds\n")
      }
      Sys.sleep(delay)
    }
    
    forecast_data = get_municipality_forecast_v3(code, city)
    
    if(!is.null(forecast_data)) {
      batch_results[[code]] = forecast_data
      successful_in_batch = successful_in_batch + 1
    }
  }
  
  cat("Batch", batch_num, "completed:", successful_in_batch, "successful out of", length(municipality_batch), "\n")
  return(batch_results)
}

# Load municipality data
cat("Loading municipality codes...\n")
municipalities_data = fread("data/input/municipalities.csv.gz")
cat("Loaded", nrow(municipalities_data), "municipalities\n")

working_municipalities = municipalities_data$CUMUN
names(working_municipalities) = municipalities_data$NAMEUNIT

if(TESTING_MODE){
  working_municipalities = head(working_municipalities, N_TEST_MUNICIPALITIES)
  cat("Testing with", N_TEST_MUNICIPALITIES, "municipalities\n\n")
}

# Split municipalities into batches for better progress tracking
total_municipalities = length(working_municipalities)
batches = split(working_municipalities, ceiling(seq_along(working_municipalities) / BATCH_SIZE))
total_batches = length(batches)

cat("Processing", total_municipalities, "municipalities in", total_batches, "batches of ~", BATCH_SIZE, "\n\n")

# Process all batches
all_forecasts = list()
total_successful = 0

for(batch_num in seq_along(batches)) {
  start_time = Sys.time()
  
  batch_results = process_municipality_batch(batches[[batch_num]], batch_num, total_batches)
  
  # Merge batch results
  all_forecasts = c(all_forecasts, batch_results)
  total_successful = total_successful + length(batch_results)
  
  end_time = Sys.time()
  batch_duration = as.numeric(difftime(end_time, start_time, units = "mins"))
  
  cat("Batch", batch_num, "duration:", round(batch_duration, 2), "minutes\n")
  cat("Total successful so far:", total_successful, "out of", batch_num * BATCH_SIZE, "\n")
  
  # Estimate remaining time
  avg_time_per_batch = batch_duration
  remaining_batches = total_batches - batch_num
  estimated_remaining = remaining_batches * avg_time_per_batch
  
  if(batch_num > 1) {
    cat("Estimated remaining time:", round(estimated_remaining, 1), "minutes\n")
  }
  cat("\n")
  
  # Save intermediate results every 10 batches
  if(batch_num %% 10 == 0 && length(all_forecasts) > 0) {
    cat("Saving intermediate results after batch", batch_num, "...\n")
    intermediate_data = do.call(rbind, all_forecasts)
    intermediate_data$collected_at = Sys.time()
    
    dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
    intermediate_file = paste0("data/output/municipal_forecasts_intermediate_", Sys.Date(), "_batch", batch_num, ".csv")
    write.csv(intermediate_data, intermediate_file, row.names = FALSE)
    cat("Intermediate data saved to:", intermediate_file, "\n\n")
  }
}

# Final results
cat("=== FINAL RESULTS ===\n")
cat("Municipalities attempted:", total_municipalities, "\n")
cat("Successful collections:", total_successful, "\n")
cat("Success rate:", round(100 * total_successful / total_municipalities, 1), "%\n")

if(length(all_forecasts) > 0) {
  final_data = do.call(rbind, all_forecasts)
  final_data$collected_at = Sys.time()
  
  cat("Total forecast records:", nrow(final_data), "\n")
  cat("Date range:", as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")
  
  # Save the final data
  dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
  output_file = paste0("data/output/municipal_forecasts_", Sys.Date(), ".csv")
  write.csv(final_data, output_file, row.names = FALSE)
  cat("Final data saved to:", output_file, "\n")
  
  # Also save as compressed version
  fwrite(final_data, paste0(output_file, ".gz"))
  cat("Compressed version saved to:", paste0(output_file, ".gz"), "\n")
  
} else {
  cat("No data collected\n")
}

cat("Completed at:", format(Sys.time()), "\n")
