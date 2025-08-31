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

# Load API keys and set for climaemet
source("auth/keys.R")
aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)

# Configuration
TESTING_MODE = FALSE
N_TEST_MUNICIPALITIES = 20  # Small test for full system

# Load municipality data 
cat("Loading municipality codes...\n")
municipalities_data = fread("data/input/municipalities.csv.gz")
all_municipios = municipalities_data$CUMUN
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
      cat("⚠️  No forecast data available for batch", batch_num, "(all municipalities may be inactive)\n")
      cat("Skipping data processing for this batch\n\n")
      next  # Skip to next batch
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
        mutate(temp_avg = rowMeans(cbind(temp_max, temp_min), na.rm = TRUE))
      
      # Get humidity data
      humidity_data = aemet_forecast_tidy(batch_forecasts, "humedadRelativa") %>%
        select(
          municipio = municipio,
          fecha,
          humid_max = humedadRelativa_maxima,
          humid_min = humedadRelativa_minima
        )
      
      # Get wind data
      wind_data = aemet_forecast_tidy(batch_forecasts, "viento") %>%
        select(
          municipio = municipio,
          fecha,
          wind_speed = viento_velocidad
        )
      
      # Combine all data
      batch_final = temp_data %>%
        left_join(humidity_data, by = c("municipio_id" = "municipio", "fecha")) %>%
        left_join(wind_data, by = c("municipio_id" = "municipio", "fecha")) %>%
        mutate(collected_at = Sys.time())
      
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
  
  cat("Total forecast records:", nrow(final_data), "\n")
  cat("Municipalities with data:", length(unique(final_data$municipio_id)), "out of", length(all_municipios), "\n")
  cat("Success rate:", round(100 * length(unique(final_data$municipio_id)) / length(all_municipios), 1), "%\n")
  cat("Date range:", as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")
  
  # Save final results
  dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
  
  # Standard CSV
  output_file = paste0("data/output/municipal_forecasts_", Sys.Date(), ".csv")
  write.csv(final_data, output_file, row.names = FALSE)
  cat("Final data saved to:", output_file, "\n")
  
  # Compressed version  
  output_file_gz = paste0(output_file, ".gz")
  fwrite(final_data, output_file_gz)
  cat("Compressed version saved to:", output_file_gz, "\n")
  
  # Summary statistics
  cat("\n=== SUMMARY STATISTICS ===\n")
  cat("Total municipalities processed:", length(unique(final_data$municipio_id)), "\n")
  cat("Total forecast days:", nrow(final_data), "\n")
  cat("Average forecasts per municipality:", round(nrow(final_data) / length(unique(final_data$municipio_id)), 1), "\n")
  
  # Show sample data
  cat("\nSample of final data:\n")
  print(head(final_data, 3))
  
} else {
  cat("❌ No data collected successfully\n")
  quit(save = "no", status = 1)
}

cat("\nCompleted at:", format(Sys.time()), "\n")
