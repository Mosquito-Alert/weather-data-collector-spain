#!/usr/bin/env Rscript

# HYBRID APPROACH: Municipal forecasts using climaemet package
# This replaces get_forecast_data.R with a much faster, more reliable solution
# Part of the 3-dataset strategy for Spanish weather data

cat("=== MUNICIPAL FORECASTS COLLECTION (CLIMAEMET) ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Load required libraries
library(climaemet)
library(dplyr)
library(data.table)
library(stringr)

# Load API keys and set for climaemet
source("auth/keys.R")
aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)

# Configuration
TESTING_MODE = FALSE
N_TEST_MUNICIPALITIES = 20  # Small test for full system

# Load municipality data 
cat("Loading municipality codes...\n")
municipalities_data = fread(
  "data/input/municipalities.csv.gz",
  colClasses = list(character = "CUMUN")
)

if(!"CUMUN" %in% names(municipalities_data)){
  stop("CUMUN column not found in municipalities.csv.gz")
}

all_municipios = str_pad(trimws(municipalities_data$CUMUN), width = 5, pad = "0")
cat("Loaded", length(all_municipios), "municipalities\n")

if(TESTING_MODE) {
  all_municipios = head(all_municipios, N_TEST_MUNICIPALITIES)
  cat("Testing mode: using", length(all_municipios), "municipalities\n")
}

# Split into batches to handle potential API limits and allow progress tracking
BATCH_SIZE = 500  # Process in smaller batches
batches = split(all_municipios, ceiling(seq_along(all_municipios) / BATCH_SIZE))
total_batches = length(batches)

cat("Processing", length(all_municipios), "municipalities in", total_batches, "batches\n")
cat("Note: Individual municipality API errors are normal - not all codes have active forecast data\n\n")

all_forecasts = list()
successful_municipalities = 0

for(batch_num in seq_along(batches)) {
  cat("=== BATCH", batch_num, "of", total_batches, "===\n")
  current_batch = batches[[batch_num]]
  
  batch_start_time = Sys.time()
  
  tryCatch({
    # Use climaemet for this batch
    cat("Collecting forecasts for", length(current_batch), "municipalities...\n")
    
    # Function to attempt forecast collection with key rotation on failure
    collect_with_retry <- function(municipios, max_retries = 3) {
      municipios = str_pad(trimws(municipios), width = 5, pad = "0")
      for(attempt in 1:max_retries) {
        tryCatch({
          # Set current API key
          aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)
          
          # Attempt to collect forecasts
          # Note: Individual municipality errors are normal - not all codes have active forecast data
          batch_forecasts = aemet_forecast_daily(
            x = municipios,
            verbose = FALSE,
            progress = TRUE
          )
          
          # Check if we got any data back
          if(is.null(batch_forecasts) || nrow(batch_forecasts) == 0) {
            cat("No forecast data returned for this batch (all municipalities may be inactive)\n")
            return(data.frame())  # Return empty data frame instead of failing
          }
          
          return(batch_forecasts)  # Success - return data
          
        }, error = function(e) {
          error_msg = as.character(e$message)
          cat("Attempt", attempt, "failed:", error_msg, "\n")
          
          # Check if error suggests rate limiting or API key issues (not individual municipality errors)
          if(grepl("429|rate limit|quota|forbidden|unauthorized|timeout|too many requests", error_msg, ignore.case = TRUE) && 
             attempt < max_retries) {
            
            cat("Detected potential rate limiting or API error. Rotating API key...\n")
            rotate_api_key()
            cat("Waiting 30 seconds before retry...\n")
            Sys.sleep(30)
            
          } else if(attempt == max_retries) {
            cat("All retry attempts failed for this batch\n")
            # Return empty data frame instead of stopping completely
            return(data.frame())
          }
        })
      }
    }
    
    # Collect forecasts with retry logic
    batch_forecasts = collect_with_retry(current_batch)
    
    # Check if we got any data from this batch
    if(is.null(batch_forecasts) || nrow(batch_forecasts) == 0) {
      cat("⚠️  No forecast data returned for batch", batch_num, "on first attempt.\n")
      cat("Trying per-municipality fallback requests...\n")

      fallback_requests = lapply(current_batch, function(mun){
        mun = str_pad(trimws(mun), width = 5, pad = "0")
        tryCatch({
          aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)
          resp = aemet_forecast_daily(x = mun, verbose = FALSE, progress = FALSE)
          if(is.null(resp) || nrow(resp) == 0){
            return(NULL)
          }
          resp
        }, error = function(e){
          cat("  Municipality", mun, "failed:", e$message, "\n")
          NULL
        })
      })

      fallback_requests = fallback_requests[!vapply(fallback_requests, is.null, logical(1))]

      if(length(fallback_requests) == 0){
        cat("⚠️  No forecast data available for batch", batch_num, "even after fallback.\n")
        cat("Skipping data processing for this batch\n\n")
        next
      }

      batch_forecasts = rbindlist(fallback_requests, fill = TRUE)
      cat("Fallback collected", nrow(batch_forecasts), "rows for batch", batch_num, "\n")
    }
    
    cat("Raw forecast collection completed for batch", batch_num, "\n")
    cat("Retrieved", nrow(batch_forecasts), "municipality-day records\n")
    
    # Extract and process data in our standard format
    cat("Processing forecast data...\n")
    
    # Check if required columns exist before processing
    tryCatch({
      # Get temperature data
      temp_data = aemet_forecast_tidy(batch_forecasts, "temperatura") %>%
        select(
          municipio_id = municipio,
          municipio_nombre = nombre,
          provincia,
          elaborado,
          fecha,
          temp_max = temperatura_maxima,
          temp_min = temperatura_minima
        ) %>%
        mutate(
          municipio_id = str_pad(as.character(municipio_id), width = 5, pad = "0")
        ) %>%
        mutate(
          temp_avg = rowMeans(cbind(temp_max, temp_min), na.rm = TRUE),
          temp_avg = ifelse(is.nan(temp_avg), NA_real_, temp_avg)
        )
      
      # Get humidity data
      humidity_data = aemet_forecast_tidy(batch_forecasts, "humedadRelativa") %>%
        select(
          municipio = municipio,
          fecha,
          humid_max = humedadRelativa_maxima,
          humid_min = humedadRelativa_minima
        ) %>%
        mutate(
          municipio = str_pad(as.character(municipio), width = 5, pad = "0")
        )
      
      # Get wind data
      wind_data = aemet_forecast_tidy(batch_forecasts, "viento") %>%
        select(
          municipio = municipio,
          fecha,
          wind_speed = viento_velocidad
        ) %>%
        mutate(
          municipio = str_pad(as.character(municipio), width = 5, pad = "0")
        )
      
      # Combine all data
      collection_time = Sys.time()
      batch_final = temp_data %>%
        left_join(humidity_data, by = c("municipio_id" = "municipio", "fecha")) %>%
        left_join(wind_data, by = c("municipio_id" = "municipio", "fecha")) %>%
        mutate(
          fecha = as.Date(fecha),
          collected_at = collection_time
        )
      
    }, error = function(e) {
      cat("Error processing forecast data for batch", batch_num, ":", e$message, "\n")
      cat("Skipping this batch\n\n")
      next
    })
    
    # Store batch results
    all_forecasts[[batch_num]] = batch_final
    successful_municipalities = successful_municipalities + length(unique(batch_final$municipio_id))
    
    batch_end_time = Sys.time()
    batch_duration = as.numeric(difftime(batch_end_time, batch_start_time, units = "mins"))
    
    cat("✅ Batch", batch_num, "completed successfully\n")
    cat("Duration:", round(batch_duration, 2), "minutes\n")
    cat("Records in batch:", nrow(batch_final), "\n")
    cat("Total successful municipalities so far:", successful_municipalities, "\n")
    
    # Estimate remaining time
    if(batch_num > 1) {
      avg_time_per_batch = batch_duration
      remaining_batches = total_batches - batch_num
      estimated_remaining = remaining_batches * avg_time_per_batch
      cat("Estimated remaining time:", round(estimated_remaining, 1), "minutes\n")
    }
    
    # Save intermediate results every 5 batches
    if(batch_num %% 5 == 0) {
      cat("Saving intermediate results after batch", batch_num, "...\n")
      intermediate_data = do.call(rbind, all_forecasts)
      
      dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
      intermediate_file = paste0("data/output/municipal_forecasts_intermediate_", Sys.Date(), "_batch", batch_num, ".csv.gz")
      fwrite(intermediate_data, intermediate_file)
      cat("Intermediate data saved to:", intermediate_file, "\n")
    }
    
    cat("\n")
    
  }, error = function(e) {
    cat("❌ Batch", batch_num, "failed:", e$message, "\n")
    cat("Continuing with next batch...\n\n")
  })
  
  # Small delay between batches to be respectful to API
  if(batch_num < total_batches) {
    Sys.sleep(2)
  }
}

# Combine all successful batches
cat("=== FINAL PROCESSING ===\n")
if(length(all_forecasts) > 0) {
  final_data = do.call(rbind, all_forecasts)
  final_data$fecha = as.Date(final_data$fecha)
  final_data$collected_at = as.POSIXct(final_data$collected_at, tz = "UTC")

  cat("Total forecast records (current run):", nrow(final_data), "\n")
  cat("Municipalities with data (current run):", length(unique(final_data$municipio_id)), "out of", length(all_municipios), "\n")
  cat("Current run date range:", as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")
  
  dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
  cumulative_path = "data/output/daily_municipal_forecast.csv.gz"
  existing_data = data.table()

  if(file.exists(cumulative_path)){
    cat("Loading existing cumulative forecast file...\n")
    existing_data = suppressWarnings(fread(cumulative_path, showProgress = FALSE))
    if(!"municipio_id" %in% names(existing_data)){
      cat("Existing file is missing municipio_id column. It will be overwritten.\n")
      existing_data = data.table()
    } else {
      if(!inherits(existing_data$fecha, "Date")){
        existing_data[, fecha := as.Date(fecha)]
      }
      if(!inherits(existing_data$collected_at, "POSIXct")){
        existing_data[, collected_at := as.POSIXct(collected_at, tz = "UTC")]
      }
    }
  }

  combined_data = rbind(existing_data, as.data.table(final_data), fill = TRUE)

  if(nrow(combined_data) > 0){
    # Keep the latest observation per municipality/date/elaborated combination
    setorderv(combined_data, c("municipio_id", "fecha", "elaborado", "collected_at"), order = c(1, 1, 1, 1), na.last = TRUE)
    combined_data = combined_data[!duplicated(combined_data, by = c("municipio_id", "fecha", "elaborado"), fromLast = TRUE)]
  }

  # Save cumulative dataset
  fwrite(combined_data, cumulative_path)
  cat("Cumulative forecast file updated:", cumulative_path, "\n")

  # Summary statistics
  cat("\n=== SUMMARY STATISTICS ===\n")
  cat("Total municipalities in cumulative file:", length(unique(combined_data$municipio_id)), "\n")
  cat("Total forecast records stored:", nrow(combined_data), "\n")
  cat("Date range stored:", as.character(min(combined_data$fecha)), "to", as.character(max(combined_data$fecha)), "\n")

  cat("\nSample of current run data:\n")
  print(head(final_data, 3))
  
} else {
  cat("❌ No data collected successfully\n")
  quit(save = "no", status = 1)
}

cat("\nCompleted at:", format(Sys.time()), "\n")
