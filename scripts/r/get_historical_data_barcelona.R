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
existing_data <- if (file.exists(output_data_file_path)) fread(output_data_file_path) else NULL

if (!is.null(existing_data)) {
  missing_dates <- setdiff(all_dates, unique(existing_data$fecha))
} else {
  missing_dates <- all_dates
}

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

# Split date range into manageable windows per station
days_per_chunk <- 30
range_starts <- seq(from = min_missing, to = max_missing, by = days_per_chunk)
range_bounds <- data.table(
  start = range_starts,
  end = pmin(range_starts + (days_per_chunk - 1), max_missing)
)

fetch_station_window <- function(station, start_date, end_date, attempt = 1L) {
  tryCatch({
    url <- sprintf(
      "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/%sT00%%3A00%%3A00UTC/fechafin/%sT23%%3A59%%3A59UTC/estacion/%s",
      start_date, end_date, station
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

    data_url <- fromJSON(rawToChar(resp$content))$datos
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
      daily_dt[[col]] <- as.numeric(str_replace(daily_dt[[col]], ",", "."))
    }

    daily_dt[, prec := fcase(
      is.na(prec), NA_real_,
      str_detect(prec, "(?i)ip"), 0.1,
      prec == "", NA_real_,
      default = suppressWarnings(as.numeric(str_replace(prec, ",", ".")))
    )]

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
    start_i <- range_bounds$start[idx]
    end_i <- range_bounds$end[idx]
    cat("  Window", idx, "-", as.character(start_i), "to", as.character(end_i), "\n")
    fetch_station_window(station, start_i, end_i)
  }), fill = TRUE)
}), fill = TRUE)

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

if (!is.null(existing_data) && nrow(existing_data)) {
  combined <- rbindlist(list(existing_data, new_records), fill = TRUE)
  setorder(combined, fecha, indicativo)
  combined <- unique(combined, by = c("fecha", "indicativo"))
} else {
  combined <- new_records[order(fecha, indicativo)]
}

fwrite(combined, output_data_file_path)
cat("Historical Barcelona dataset updated:", output_data_file_path, "\n")
cat("Total records stored:", nrow(combined), "\n")
cat("Date range:", as.character(min(combined$fecha)), "to", as.character(max(combined$fecha)), "\n")
