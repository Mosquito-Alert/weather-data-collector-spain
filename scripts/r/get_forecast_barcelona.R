#!/usr/bin/env Rscript

# Quick helper to fetch and store the Barcelona municipality forecast
# (Municipio code 08019). Mirrors the nationwide workflow with persistence,
# locking, and elaborado freshness guards so the Barcelona-only pipeline can
# run independently.

cat("=== BARCELONA FORECAST COLLECTION (CLIMAEMET) ===\n")
cat("Started at:", format(Sys.time(), tz = "UTC"), "UTC\n\n")

suppressPackageStartupMessages({
  library(climaemet)
  library(dplyr)
  library(data.table)
  library(stringr)
  library(lubridate)
})

`%||%` <- function(x, y) {
  if (is.null(x) || (is.character(x) && !length(x)) || (length(x) && all(is.na(x)))) return(y)
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

as_int_or_default <- function(value, default) {
  if (is.null(value)) return(default)
  parsed <- suppressWarnings(as.integer(value))
  if (length(parsed) != 1L || is.na(parsed)) return(default)
  parsed
}

safe_parse_elaborado <- function(x) {
  if (is.null(x)) return(as.POSIXct(NA))
  parsed <- suppressWarnings(as_datetime(x, tz = "UTC"))
  parsed
}

safe_max_datetime <- function(x) {
  if (!length(x)) return(as.POSIXct(NA))
  suppressWarnings({
    out <- max(x, na.rm = TRUE)
    if (is.infinite(out)) as.POSIXct(NA) else out
  })
}

# Load rotating API key helpers
source("auth/keys.R")

cli_args <- parse_cli_args(commandArgs(trailingOnly = TRUE))

if (!is.null(cli_args[["key-pool"]])) {
  pool_names <- strsplit(cli_args[["key-pool"]], ",", fixed = TRUE)[[1]]
  pool_names <- trimws(pool_names)
  pool_names <- pool_names[nzchar(pool_names)]
  if (!length(pool_names)) {
    stop("key-pool argument provided but no valid key identifiers were found.")
  }
  set_active_key_pool(pool_names)
}

cat("Active API key pool:", paste(get_active_key_pool(), collapse = ", "), "\n")

# Configuration ------------------------------------------------------------
municipio_id <- "08019"  # Barcelona
max_retries_api <- 5
retry_wait_seconds <- 30
elaborado_wait_seconds <- as_int_or_default(cli_args[["elaborado-wait-seconds"]], 900)
elaborado_max_attempts <- max(1L, as_int_or_default(cli_args[["elaborado-max-attempts"]], 4L))
output_dir <- "data/output"
output_file <- file.path(output_dir, "daily_municipal_forecast_barcelona.csv.gz")
lock_path <- paste0(output_file, ".lock")
LOCK_TIMEOUT_SECONDS <- 600
LOCK_SLEEP_SECONDS <- 1

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
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

load_existing_forecasts <- function(path) {
  if (!file.exists(path)) return(data.table())
  dt <- suppressWarnings(fread(path, showProgress = FALSE))
  if (!nrow(dt)) return(dt)
  if (!"municipio_id" %in% names(dt)) dt[, municipio_id := municipio %||% "08019"]
  dt[, municipio_id := str_pad(as.character(municipio_id), width = 5, pad = "0")]
  if (!inherits(dt$fecha, "Date")) dt[, fecha := as.Date(fecha)]
  if (!"collected_at" %in% names(dt)) dt[, collected_at := as.POSIXct(NA)]
  if (!inherits(dt$collected_at, "POSIXct")) dt[, collected_at := as.POSIXct(collected_at, tz = "UTC")]
  if (!"elaborado" %in% names(dt)) dt[, elaborado := NA_character_]
  dt[, elaborado_dt := safe_parse_elaborado(elaborado)]
  dt
}

persist_forecasts <- function(batch_dt) {
  acquire_file_lock(lock_path)
  on.exit(release_file_lock(lock_path), add = TRUE)
  current <- load_existing_forecasts(output_file)
  combined <- rbindlist(list(current, batch_dt), use.names = TRUE, fill = TRUE)
  combined[, `:=`(
    municipio_id = str_pad(as.character(municipio_id), width = 5, pad = "0"),
    fecha = as.Date(fecha),
    collected_at = as.POSIXct(collected_at, tz = "UTC"),
    elaborado_dt = safe_parse_elaborado(elaborado)
  )]
  setorderv(combined, c("municipio_id", "fecha", "elaborado_dt", "collected_at"))
  combined <- unique(combined, by = c("municipio_id", "fecha", "elaborado"), fromLast = TRUE)
  write_dt <- copy(combined)
  if ("elaborado_dt" %in% names(write_dt)) write_dt[, elaborado_dt := NULL]
  fwrite(write_dt, output_file)
  combined
}

# Helper: attempt forecast with retries and API key rotation
retrieve_forecast <- function(id) {
  id <- str_pad(trimws(id), width = 5, pad = "0")
  for (attempt in seq_len(max_retries_api)) {
    aemet_api_key(get_current_api_key(), install = TRUE, overwrite = TRUE)
    cat("Attempt", attempt, "using active API key\n")
    tryCatch({
      res <- aemet_forecast_daily(x = id, verbose = FALSE, progress = FALSE)
      if (is.null(res) || nrow(res) == 0) {
        stop("No data returned for municipality ", id)
      }
      return(res)
    }, error = function(e) {
      cat("  Attempt", attempt, "failed:", conditionMessage(e), "\n")
      if (attempt < max_retries_api) {
        cat("  Rotating API key and waiting", retry_wait_seconds, "seconds...\n")
        rotate_api_key()
        Sys.sleep(retry_wait_seconds)
      }
    })
  }
  stop("Unable to retrieve forecast for municipality ", id, " after ", max_retries_api, " attempts")
}

tidy_forecast <- function(raw_forecast, collection_time) {
  extract_block <- function(data, block) {
    aemet_forecast_tidy(data, block)
  }

  temp_data <- extract_block(raw_forecast, "temperatura") %>%
    transmute(
      municipio_id = str_pad(as.character(municipio), width = 5, pad = "0"),
      municipio_nombre = nombre,
      provincia,
      elaborado,
      fecha = as.Date(fecha),
      temp_max = temperatura_maxima,
      temp_min = temperatura_minima,
      temp_avg = {
        avg <- rowMeans(cbind(temperatura_maxima, temperatura_minima), na.rm = TRUE)
        ifelse(is.nan(avg), NA_real_, avg)
      }
    )

  humidity_data <- extract_block(raw_forecast, "humedadRelativa") %>%
    transmute(
      municipio = str_pad(as.character(municipio), width = 5, pad = "0"),
      fecha = as.Date(fecha),
      humid_max = humedadRelativa_maxima,
      humid_min = humedadRelativa_minima
    )

  wind_data <- extract_block(raw_forecast, "viento") %>%
    transmute(
      municipio = str_pad(as.character(municipio), width = 5, pad = "0"),
      fecha = as.Date(fecha),
      wind_speed = viento_velocidad
    )

  temp_data %>%
    left_join(humidity_data, by = c("municipio_id" = "municipio", "fecha")) %>%
    left_join(wind_data, by = c("municipio_id" = "municipio", "fecha")) %>%
    mutate(
      collected_at = collection_time,
      elaborado_dt = safe_parse_elaborado(elaborado)
    )
}

existing_forecasts <- load_existing_forecasts(output_file)
existing_max_elaborado <- safe_max_datetime(existing_forecasts$elaborado_dt)
if (!is.na(existing_max_elaborado)) {
  cat("Latest stored elaborado:", format(existing_max_elaborado, tz = "UTC"), "UTC\n")
}

final_batch <- NULL

for (attempt in seq_len(elaborado_max_attempts)) {
  collection_time <- Sys.time()
  raw_forecast <- retrieve_forecast(municipio_id)
  cat("Successfully retrieved", nrow(raw_forecast), "rows of raw forecast data\n")
  batch_final <- tidy_forecast(raw_forecast, collection_time)
  if (!nrow(batch_final)) {
    cat("No forecast rows returned after tidying.\n")
    final_batch <- batch_final
    break
  }

  new_max_elaborado <- safe_max_datetime(batch_final$elaborado_dt)
  if (is.na(existing_max_elaborado) || is.na(new_max_elaborado) || new_max_elaborado > existing_max_elaborado || attempt == elaborado_max_attempts) {
    if (!is.na(existing_max_elaborado) && !is.na(new_max_elaborado) && new_max_elaborado <= existing_max_elaborado && attempt == elaborado_max_attempts) {
      cat("Proceeding with same elaborado timestamp after", elaborado_max_attempts, "attempts.\n")
    }
    final_batch <- batch_final
    break
  }

  wait_time <- elaborado_wait_seconds
  cat("Latest elaborado", format(new_max_elaborado, tz = "UTC"), "UTC not newer than stored",
      format(existing_max_elaborado, tz = "UTC"), "UTC. Waiting", wait_time, "seconds before retry...\n")
  Sys.sleep(wait_time)
}

if (is.null(final_batch) || !nrow(final_batch)) {
  cat("No forecast data available for Barcelona at this time.\n")
  quit(status = 0)
}

final_data_dt <- as.data.table(final_batch)
final_data_dt[, collected_at := as.POSIXct(collected_at, tz = "UTC")]

combined <- persist_forecasts(final_data_dt)

new_max_elaborado <- safe_max_datetime(final_data_dt$elaborado_dt)
cat("Final tidy forecasts:", nrow(final_data_dt), "rows spanning",
    as.character(min(final_data_dt$fecha)), "to", as.character(max(final_data_dt$fecha)), "\n")
if (!is.na(new_max_elaborado)) {
  cat("Latest elaborado in this run:", format(new_max_elaborado, tz = "UTC"), "UTC\n")
}

cat("Barcelona forecast saved to:", output_file, "\n")
cat("Cumulative rows stored:", nrow(combined), "\n")
cat("Municipality forecast date range:", as.character(min(combined$fecha, na.rm = TRUE)), "to",
    as.character(max(combined$fecha, na.rm = TRUE)), "\n")

cat("\nCompleted at:", format(Sys.time(), tz = "UTC"), "UTC\n")
