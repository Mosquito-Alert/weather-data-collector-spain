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

`%||%` <- function(x, y) {
  if (is.null(x) || isTRUE(is.na(x)) || (is.character(x) && identical(x, ""))) return(y)
  x
}

parse_cli_args <- function(args) {
  if (!length(args)) return(list())
  parsed <- list()
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    stripped <- substr(arg, 3, nchar(arg))
    parts <- strsplit(stripped, "=", fixed = TRUE)[[1]]
    key <- parts[1]
    value <- if (length(parts) > 1) parts[2] else TRUE
    parsed[[key]] <- value
  }
  parsed
}

# Configuration
RUN_DATE <- as.Date(Sys.time(), tz = "UTC")
BATCH_SIZE <- 100
BATCH_PAUSE_SECONDS <- 8
cumulative_path <- "data/output/daily_municipal_forecast.csv.gz"
dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
lock_path <- paste0(cumulative_path, ".lock")
LOCK_TIMEOUT_SECONDS <- 600
LOCK_SLEEP_SECONDS <- 1

# Load API keys and set for climaemet
source("auth/keys.R")

cli_args <- parse_cli_args(commandArgs(trailingOnly = TRUE))

as_int_or_default <- function(value, default) {
  if (is.null(value)) return(default)
  parsed <- suppressWarnings(as.integer(value))
  if (length(parsed) != 1L || is.na(parsed)) return(default)
  parsed
}

if (!is.null(cli_args[["key-pool"]])) {
  pool_names <- strsplit(cli_args[["key-pool"]], ",", fixed = TRUE)[[1]]
  pool_names <- trimws(pool_names)
  pool_names <- pool_names[nzchar(pool_names)]
  if (!length(pool_names)) {
    stop("key-pool argument provided but no valid key identifiers were found.")
  }
  set_active_key_pool(pool_names)
}

shard_index <- as_int_or_default(cli_args[["shard-index"]], 1L)
shard_count <- as_int_or_default(cli_args[["shard-count"]], 1L)

if (shard_index < 1L) {
  stop("shard-index must be a positive integer.")
}
if (shard_count < 1L) {
  stop("shard-count must be a positive integer.")
}
if (shard_index > shard_count) {
  stop("shard-index cannot exceed shard-count.")
}

aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)
cat("Active API key pool:", paste(get_active_key_pool(), collapse = ", "), "\n")
cat("Shard assignment: index", shard_index, "of", shard_count, "\n")

# Configuration
TESTING_MODE = FALSE
N_TEST_MUNICIPALITIES = 20  # Small test for full system

load_cumulative_data <- function(path) {
  if (!file.exists(path)) return(data.table())
  dt <- suppressWarnings(fread(path, showProgress = FALSE))
  if (!nrow(dt)) return(dt)
  if (!"municipio_id" %in% names(dt)) {
    dt[, municipio_id := NA_character_]
  }
  if (!inherits(dt$fecha, "Date")) {
    dt[, fecha := as.Date(fecha)]
  }
  if (!"collected_at" %in% names(dt)) {
    dt[, collected_at := as.POSIXct(NA)]
  }
  if (!inherits(dt$collected_at, "POSIXct")) {
    dt[, collected_at := as.POSIXct(collected_at, tz = "UTC")]
  }
  dt
}

cumulative_data <- load_cumulative_data(cumulative_path)
completed_today <- character()
if (nrow(cumulative_data)) {
  completed_today <- cumulative_data[
    !is.na(municipio_id) & as.Date(collected_at, tz = "UTC") == RUN_DATE,
    unique(municipio_id)
  ]
  if (length(completed_today)) {
    cat("Already collected", length(completed_today), "municipalities for", RUN_DATE, "\n")
  }
}

save_cumulative_data <- function(path, dt) {
  tmp_path <- paste0(path, ".tmp")
  fwrite(dt, tmp_path)
  file.rename(tmp_path, path)
}

acquire_file_lock <- function(path, timeout = LOCK_TIMEOUT_SECONDS, sleep = LOCK_SLEEP_SECONDS) {
  start_time <- Sys.time()
  repeat {
    if (!file.exists(path)) {
      if (file.create(path)) {
        return(TRUE)
      }
    }
    if (difftime(Sys.time(), start_time, units = "secs") > timeout) {
      stop("Unable to acquire file lock at ", path, " within timeout.")
    }
    Sys.sleep(sleep)
  }
}

release_file_lock <- function(path) {
  if (file.exists(path)) {
    unlink(path)
  }
}

persist_batch <- function(batch_dt) {
  if (!nrow(batch_dt)) return()
  batch_dt[, collected_at := as.POSIXct(collected_at, tz = "UTC")]
  acquire_file_lock(lock_path)
  on.exit(release_file_lock(lock_path), add = TRUE)
  latest_disk <- load_cumulative_data(cumulative_path)
  combined <- rbind(latest_disk, batch_dt, fill = TRUE)
  setorderv(combined, c("municipio_id", "fecha", "elaborado", "collected_at"))
  combined <- unique(combined, by = c("municipio_id", "fecha", "elaborado"), fromLast = TRUE)
  cumulative_data <<- combined
  save_cumulative_data(cumulative_path, cumulative_data)
}

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

if (shard_count > 1L) {
  shard_groups <- split(all_municipios, ((seq_along(all_municipios) - 1L) %% shard_count) + 1L)
  all_municipios <- shard_groups[[shard_index]]
  cat("Shard", shard_index, "assigned", length(all_municipios), "municipalities\n")
}

if (!length(all_municipios)) {
  cat("No municipalities assigned to this shard. Exiting.\n")
  quit(save = "no", status = 0)
}

assigned_municipios_total <- length(all_municipios)

remaining_municipios <- setdiff(all_municipios, completed_today)
if (!length(remaining_municipios)) {
  cat("All municipalities already collected for", RUN_DATE, "- exiting early.\n")
  quit(save = "no", status = 0)
}

# Split into batches to handle potential API limits and allow progress tracking
batch_size <- if (TESTING_MODE) min(BATCH_SIZE, length(remaining_municipios)) else BATCH_SIZE
batches = split(remaining_municipios, ceiling(seq_along(remaining_municipios) / batch_size))
total_batches = length(batches)

cat("Processing", length(remaining_municipios), "municipalities in", total_batches, "batches\n")
cat("Note: Individual municipality API errors are normal - not all codes have active forecast data\n\n")

all_forecasts = list()
successful_municipalities = 0
processed_in_run <- character()

for(batch_num in seq_along(batches)) {
  cat("=== BATCH", batch_num, "of", total_batches, "===\n")
  current_batch = batches[[batch_num]]
  if (!length(current_batch)) {
    cat("Batch", batch_num, "has no municipalities remaining. Skipping.\n\n")
    next
  }
  current_batch = intersect(current_batch, remaining_municipios)
  if (!length(current_batch)) {
    cat("All municipalities in this batch were already processed earlier today. Skipping.\n\n")
    next
  }
  
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
            progress = FALSE
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
      cat("Trying fallback with smaller sub-batches...\n")

      SUB_BATCH_SIZE = 50
      sub_batches = split(current_batch, ceiling(seq_along(current_batch) / SUB_BATCH_SIZE))
      fallback_results = lapply(seq_along(sub_batches), function(sub_idx){
        sub_batch = sub_batches[[sub_idx]]
        cat("  Sub-batch", sub_idx, "of", length(sub_batches), "with", length(sub_batch), "municipalities...\n")
        sub_result = collect_with_retry(sub_batch)
        if(is.null(sub_result) || nrow(sub_result) == 0){
          cat("    Sub-batch", sub_idx, "returned no data.\n")
          return(NULL)
        }
        sub_result
      })

      fallback_results = fallback_results[!vapply(fallback_results, is.null, logical(1))]

      if(length(fallback_results) == 0){
        cat("⚠️  No forecast data available for batch", batch_num, "even after fallback.\n")
        cat("Skipping data processing for this batch\n\n")
        next
      }

      batch_forecasts = bind_rows(fallback_results)
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
    
    batch_final_dt <- as.data.table(batch_final)
    if (!nrow(batch_final_dt)) {
      cat("No records produced after processing batch", batch_num, "- skipping persistence.\n\n")
      next
    }

    persist_batch(batch_final_dt)
    all_forecasts[[length(all_forecasts) + 1]] <- batch_final_dt

    processed_in_run <<- union(processed_in_run, unique(batch_final_dt$municipio_id))
    successful_municipalities <<- length(processed_in_run)
    remaining_municipios <<- setdiff(remaining_municipios, unique(batch_final_dt$municipio_id))

    batch_end_time = Sys.time()
    batch_duration = as.numeric(difftime(batch_end_time, batch_start_time, units = "mins"))

    cat("✅ Batch", batch_num, "completed successfully\n")
    cat("Duration:", round(batch_duration, 2), "minutes\n")
    cat("Records in batch:", nrow(batch_final_dt), "\n")
    cat("Municipalities completed in this run:", successful_municipalities, "\n")
    cat("Municipalities remaining today:", length(remaining_municipios), "\n")

    if (batch_num < total_batches && length(remaining_municipios) > 0 && BATCH_PAUSE_SECONDS > 0) {
      cat("Pausing", BATCH_PAUSE_SECONDS, "seconds before next batch to respect API limits...\n")
      Sys.sleep(BATCH_PAUSE_SECONDS)
    }

    cat("\n")
    
  }, error = function(e) {
    cat("❌ Batch", batch_num, "failed:", e$message, "\n")
    cat("Continuing with next batch...\n\n")
  })

  if (!length(remaining_municipios)) {
    cat("All municipalities collected for today. Ending early.\n")
    break
  }
}

# Combine all successful batches
cat("=== FINAL PROCESSING ===\n")
if(length(all_forecasts) > 0) {
  final_data <- rbindlist(all_forecasts, use.names = TRUE, fill = TRUE)
  if (nrow(final_data)) {
    final_data[, fecha := as.Date(fecha)]
    final_data[, collected_at := as.POSIXct(collected_at, tz = "UTC")]
  }

  cat("Total forecast records saved this run:", nrow(final_data), "\n")
  cat("Municipalities updated this run:", length(unique(final_data$municipio_id)), "out of", assigned_municipios_total, "\n")
  if (nrow(final_data)) {
    cat("Current run date range:", as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")
  }

  cat("Cumulative forecast file updated:", cumulative_path, "\n")
} else {
  cat("No new forecast data collected in this run (municipalities may already be up to date or all API calls failed).\n")
}

if (nrow(cumulative_data)) {
  cat("\n=== SUMMARY STATISTICS ===\n")
  cat("Total municipalities in cumulative file:", length(unique(cumulative_data$municipio_id)), "\n")
  cat("Total forecast records stored:", nrow(cumulative_data), "\n")
  cat("Date range stored:", as.character(min(cumulative_data$fecha, na.rm = TRUE)), "to", as.character(max(cumulative_data$fecha, na.rm = TRUE)), "\n")
  todays_total <- cumulative_data[as.Date(collected_at, tz = "UTC") == RUN_DATE, uniqueN(municipio_id)]
  cat("Municipalities collected today (", RUN_DATE, "): ", todays_total, "\n", sep = "")
}

if (length(remaining_municipios)) {
  cat("\nMunicipalities still outstanding for", RUN_DATE, ":", length(remaining_municipios), "\n")
  cat("Re-run later today to finish the remaining municipalities once API limits reset.\n")
} else {
  cat("\nAll municipalities collected for", RUN_DATE, "✅\n")
}

cat("\nCompleted at:", format(Sys.time()), "\n")
