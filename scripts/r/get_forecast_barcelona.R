#!/usr/bin/env Rscript

# Quick helper to fetch and store only the Barcelona municipality forecast
# (Municipio code 08019). Designed for fast, direct retrieval when the
# full municipal pipeline would take too long.

cat("=== BARCELONA FORECAST COLLECTION (CLIMAEMET) ===\n")
cat("Started at:", format(Sys.time(), tz = "UTC"), "UTC\n\n")

suppressPackageStartupMessages({
  library(climaemet)
  library(dplyr)
  library(data.table)
  library(stringr)
})

# Load rotating API key helpers
source("auth/keys.R")

# Configuration ------------------------------------------------------------
municipio_id <- "08019"  # Barcelona
max_retries  <- 5
wait_seconds <- 30
output_dir   <- "data/output"
output_file  <- file.path(output_dir, "daily_municipal_forecast_barcelona.csv.gz")

# Ensure directory exists
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}

# Helper: attempt forecast with retries and API key rotation
retrieve_forecast <- function(id) {
  id <- str_pad(trimws(id), width = 5, pad = "0")
  for (attempt in seq_len(max_retries)) {
    aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)
    cat("Attempt", attempt, "using API key:", get_current_api_key(), "\n")
    tryCatch({
      res <- aemet_forecast_daily(x = id, verbose = FALSE, progress = FALSE)
      if (is.null(res) || nrow(res) == 0) {
        stop("No data returned for municipality ", id)
      }
      return(res)
    }, error = function(e) {
      cat("  Attempt", attempt, "failed:", conditionMessage(e), "\n")
      if (attempt < max_retries) {
        cat("  Rotating API key and waiting", wait_seconds, "seconds...\n")
        rotate_api_key()
        Sys.sleep(wait_seconds)
      }
    })
  }
  stop("Unable to retrieve forecast for municipality ", id, " after ", max_retries, " attempts")
}

# Fetch raw forecast ------------------------------------------------------
raw_forecast <- retrieve_forecast(municipio_id)
cat("Successfully retrieved", nrow(raw_forecast), "rows of raw forecast data\n")

# Tidy key variables ------------------------------------------------------
collection_time <- Sys.time()

extract_block <- function(data, block) {
  aemet_forecast_tidy(data, block)
}

temp_data <- extract_block(raw_forecast, "temperatura") %>%
  transmute(
    municipio_id   = str_pad(as.character(municipio), width = 5, pad = "0"),
    municipio_nombre = nombre,
    provincia,
    elaborado,
    fecha          = as.Date(fecha),
    temp_max       = temperatura_maxima,
    temp_min       = temperatura_minima,
    temp_avg       = {
      avg <- rowMeans(cbind(temperatura_maxima, temperatura_minima), na.rm = TRUE)
      ifelse(is.nan(avg), NA_real_, avg)
    }
  )

humidity_data <- extract_block(raw_forecast, "humedadRelativa") %>%
  transmute(
    municipio = str_pad(as.character(municipio), width = 5, pad = "0"),
    fecha     = as.Date(fecha),
    humid_max = humedadRelativa_maxima,
    humid_min = humedadRelativa_minima
  )

wind_data <- extract_block(raw_forecast, "viento") %>%
  transmute(
    municipio = str_pad(as.character(municipio), width = 5, pad = "0"),
    fecha     = as.Date(fecha),
    wind_speed = viento_velocidad
  )

final_data <- temp_data %>%
  left_join(humidity_data, by = c("municipio_id" = "municipio", "fecha")) %>%
  left_join(wind_data,    by = c("municipio_id" = "municipio", "fecha")) %>%
  mutate(collected_at = collection_time)

cat("Final tidy forecasts:", nrow(final_data), "rows spanning",
    as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")

# Save output -------------------------------------------------------------
fwrite(final_data, output_file)
cat("Barcelona forecast saved to:", output_file, "\n")

cat("Sample rows:\n")
print(final_data %>% select(municipio_id, fecha, temp_min, temp_max, humid_min, humid_max, wind_speed) %>% head())

cat("\nCompleted at:", format(Sys.time(), tz = "UTC"), "UTC\n")
