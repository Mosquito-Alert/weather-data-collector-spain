#!/usr/bin/env Rscript

# get_historical_data_barcelona.R
# --------------------------------
# Purpose: Quickly download/refresh historical daily weather data for the
# Barcelona municipality (AEMET municipio code 08019) while keeping the
# original AEMET field names for data integrity.

rm(list = ls())

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(jsonlite)
  library(curl)
})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
output_data_file_path <- "data/output/daily_station_historical_barcelona.csv.gz"
start_date <- as_date("2013-07-01")
bcn_natcode_suffix <- "08019"

# Load API helpers
source("auth/keys.R")

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

# Identify Barcelona station codes using municipality mapping
station_map <- fread(
  "data/input/station_point_municipaities_table.csv",
  colClasses = list(character = c("INDICATIVO", "NATCODE", "NAMEUNIT"))
)

station_map_bcn <- station_map[str_detect(NATCODE, paste0(bcn_natcode_suffix, "$"))]
barcelona_stations <- unique(station_map_bcn$INDICATIVO)

if (length(barcelona_stations) == 0L) {
  stop("No station codes found for Barcelona (08019). Check station mapping file.")
}

cat("Barcelona station codes (indicativo):", paste(barcelona_stations, collapse = ", "), "\n")

# Determine missing date range
all_dates <- seq.Date(from = start_date, to = today() - 4, by = "day")

load_existing_data <- function(path) {
  if (!file.exists(path)) return(data.table())
  dt <- fread(path)
  if ("fecha" %in% names(dt)) dt[, fecha := as_date(fecha)]
  dt
}
existing_data <- load_existing_data(output_data_file_path)

if (nrow(existing_data)) {
  missing_dates <- setdiff(all_dates, unique(existing_data$fecha))
} else {
  missing_dates <- all_dates
}

missing_dates <- as_date(missing_dates)

if (length(missing_dates) == 0L) {
  cat("Historical Barcelona dataset already up to date.\n")
  quit(status = 0)
}

min_missing <- min(missing_dates)
max_missing <- max(missing_dates)

# Prepare curl handle with API key
aemet_handle <- new_handle()
handle_setheaders(aemet_handle, "api_key" = get_current_api_key())
handle_setopt(aemet_handle, timeout = 60, connecttimeout = 30)
initial_key_label <- get_active_key_pool()[1]
cat("Using initial API key label:", initial_key_label, "\n")

safe_chr <- function(x, fallback = NA_character_) {
  if (is.null(x) || !length(x)) return(fallback)
  val <- x[1]
  if (is.na(val)) return(fallback)
  val <- trimws(as.character(val))
  if (!nzchar(val) || identical(val, "NA")) fallback else val
}

# Split date range into manageable windows per station
days_per_chunk <- 30
range_starts <- seq(from = min_missing, to = max_missing, by = days_per_chunk)
range_bounds <- data.table(
  start = as_date(range_starts),
  end = as_date(pmin(range_starts + (days_per_chunk - 1), max_missing))
)

fetch_station_window <- function(station, start_date, end_date, attempt = 1L) {
  start_date <- as_date(start_date)
  end_date <- as_date(end_date)
  if (is.na(start_date) || is.na(end_date)) {
    stop("fetch_station_window received non-date inputs (start:", start_date, ", end:", end_date, ")")
  }
  start_str <- format(start_date, "%Y-%m-%d")
  end_str <- format(end_date, "%Y-%m-%d")
  tryCatch({
    url <- sprintf(
      "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/%sT00%%3A00%%3A00UTC/fechafin/%sT23%%3A59%%3A59UTC/estacion/%s",
      start_str, end_str, station
    )

    resp <- curl_fetch_memory(url, handle = aemet_handle)
    if (resp$status_code == 429) {
      cat("Rate limit (station", station, ",", start_date, "-", end_date, ") attempt", attempt, "- rotating key...\n")
      rotate_api_key()
      handle_setheaders(aemet_handle, "api_key" = get_current_api_key())
      Sys.sleep(3)
      return(fetch_station_window(station, start_date, end_date, attempt + 1L))
    }

    if (resp$status_code != 200) {
      stop("API request failed with status ", resp$status_code)
    }

    payload <- tryCatch(fromJSON(rawToChar(resp$content)), error = identity)
    if (inherits(payload, "error")) {
      stop("Unable to parse AEMET response JSON: ", conditionMessage(payload))
    }
    data_url <- safe_chr(payload$datos)
    if (is.na(data_url) || !nzchar(data_url)) {
      status_code <- safe_chr(payload$estado, "?")
      descr <- safe_chr(payload$descripcion, "missing datos URL")
      if (status_code == "404") {
        cat("No historical data available for", station, "between", start_str, "and", end_str, "(", descr, ").\n")
        return(data.table())
      }
      stop("AEMET response missing datos URL (estado ", status_code, ": ", descr, ")")
    }

    raw_resp <- curl_fetch_memory(data_url)
    raw_txt <- rawToChar(raw_resp$content)
    Encoding(raw_txt) <- "latin1"

    daily_dt <- fromJSON(raw_txt) |> as.data.table()
    if (!nrow(daily_dt)) return(data.table())

    daily_dt[, fecha := as_date(fecha)]

    keep_cols <- c(
      "fecha", "indicativo", "nombre", "provincia", "altitud", "tmed",
      "tmax", "tmin", "hrMedia", "hrMax", "hrMin", "prec", "velmedia",
      "racha", "presMax", "presMin"
    )

    missing_cols <- setdiff(keep_cols, names(daily_dt))
    if (length(missing_cols)) {
      for (mc in missing_cols) daily_dt[, (mc) := NA]
    }

    daily_dt <- daily_dt[, ..keep_cols]

    num_cols <- c("tmed", "tmax", "tmin", "hrMedia", "hrMax", "hrMin",
                  "velmedia", "racha", "presMax", "presMin")
    for (col in num_cols) {
      col_values <- as.character(daily_dt[[col]])
      col_values[col_values %in% c("", " ", "NA")] <- NA_character_
      daily_dt[[col]] <- suppressWarnings(as.numeric(str_replace(col_values, ",", ".")))
    }

    daily_dt[, prec := {
      prec_chr <- as.character(prec)
      fcase(
        is.na(prec_chr) | prec_chr %in% c("", " ", "NA"), NA_real_,
        str_detect(prec_chr, "(?i)ip"), 0.1,
        default = suppressWarnings(as.numeric(str_replace(prec_chr, ",", ".")))
      )
    }]

    return(daily_dt)

  }, error = function(e) {
    cat("ERROR for station", station, start_date, "-", end_date, ":", conditionMessage(e), "\n")
    if (attempt >= 3L) return(data.table())
    rotate_api_key()
    handle_setheaders(aemet_handle, "api_key" = get_current_api_key())
    Sys.sleep(3)
    fetch_station_window(station, start_date, end_date, attempt + 1L)
  })
}

new_records <- rbindlist(lapply(barcelona_stations, function(station) {
  cat("Station", station, ":", nrow(range_bounds), "windows to fetch\n")
  rbindlist(lapply(seq_len(nrow(range_bounds)), function(idx) {
    start_i <- as_date(range_bounds$start[idx])
    end_i <- as_date(range_bounds$end[idx])
    cat("  Window", idx, "-", format(start_i, "%Y-%m-%d"), "to", format(end_i, "%Y-%m-%d"), "\n")
    fetch_station_window(station, start_i, end_i)
  }), fill = TRUE)
}), fill = TRUE)

if (nrow(new_records) == 0L || ncol(new_records) == 0L) {
  cat("No new Barcelona historical records downloaded.\n")
  quit(status = 0)
}

new_records <- merge(
  new_records,
  unique(station_map_bcn[, .(indicativo = INDICATIVO, municipio_natcode = NATCODE, municipio_name = NAMEUNIT)]),
  by = "indicativo",
  all.x = TRUE
)

if (!nrow(new_records)) {
  cat("No new Barcelona historical records downloaded.\n")
  quit(status = 0)
}

lock_path <- paste0(output_data_file_path, ".lock")
LOCK_TIMEOUT_SECONDS <- 600
LOCK_SLEEP_SECONDS <- 1

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

persist_records <- function(new_rows) {
  acquire_file_lock(lock_path)
  on.exit(release_file_lock(lock_path), add = TRUE)
  current <- load_existing_data(output_data_file_path)
  combined <- rbindlist(list(current, new_rows), fill = TRUE)
  setorder(combined, fecha, indicativo)
  combined <- unique(combined, by = c("fecha", "indicativo"))
  fwrite(combined, output_data_file_path)
  combined
}

combined <- persist_records(new_records)

cat("Historical Barcelona dataset updated:", output_data_file_path, "\n")
cat("Total records stored:", nrow(combined), "\n")
cat("Date range:", as.character(min(combined$fecha)), "to", as.character(max(combined$fecha)), "\n")
