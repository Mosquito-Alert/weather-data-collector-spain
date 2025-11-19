#!/usr/bin/env Rscript

# get_latest_data_barcelona.R
# ----------------------------
# Purpose: Download the latest hourly observations for Barcelona (municipio 08019)
# from the AEMET OpenData API, keeping the long-format structure used by the
# main pipeline but restricted to Barcelona station codes only.

rm(list = ls())

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(curl)
  library(jsonlite)
  library(lubridate)
})

source("auth/keys.R")

output_path <- "data/output/hourly_station_ongoing_barcelona.csv.gz"
bcn_natcode_suffix <- "08019"

safe_chr <- function(x, fallback = NA_character_) {
  if (is.null(x) || !length(x)) return(fallback)
  val <- x[1]
  if (is.na(val)) return(fallback)
  val <- trimws(as.character(val))
  if (!nzchar(val) || identical(val, "NA")) fallback else val
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

station_map <- fread(
  "data/input/station_point_municipaities_table.csv",
  colClasses = list(character = c("INDICATIVO", "NATCODE", "NAMEUNIT"))
)

station_map_bcn <- station_map[str_detect(NATCODE, paste0(bcn_natcode_suffix, "$"))]
barcelona_stations <- unique(station_map_bcn$INDICATIVO)

if (length(barcelona_stations) == 0L) {
  stop("No station codes found for Barcelona (08019).")
}

cat("Collecting hourly observations for Barcelona stations:", paste(barcelona_stations, collapse = ", "), "\n")

if (!dir.exists("data/output")) {
  dir.create("data/output", recursive = TRUE)
}

lock_path <- paste0(output_path, ".lock")
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

handle <- new_handle()
handle_setheaders(handle, "api_key" = get_current_api_key())

initial_key_label <- get_active_key_pool()[1]
cat("Using initial API key label:", initial_key_label, "\n")

fetch_station_latest <- function(station, attempt = 1L) {
  tryCatch({
    url <- sprintf("https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/%s", station)
    req <- curl_fetch_memory(url, handle = handle)
    if (req$status_code == 429) {
      stop("Rate limit")
    }
    if (req$status_code != 200) {
      stop("Status ", req$status_code)
    }
    payload <- tryCatch(fromJSON(rawToChar(req$content)), error = identity)
    if (inherits(payload, "error")) {
      stop("Unable to parse AEMET response JSON: ", conditionMessage(payload))
    }
    data_url <- safe_chr(payload$datos)
    if (is.na(data_url) || !nzchar(data_url)) {
      status_code <- safe_chr(payload$estado, "?")
      descr <- safe_chr(payload$descripcion, "missing datos URL")
      if (status_code == "404") {
        cat("Station", station, "returned 404 (", descr, "); skipping recent data.\n")
        return(data.table())
      }
      stop("AEMET response missing datos URL (estado ", status_code, ": ", descr, ")")
    }
    raw <- curl_fetch_memory(data_url)
    txt <- rawToChar(raw$content)
    Encoding(txt) <- "latin1"
    out <- fromJSON(txt) |> as.data.table()
    if (!nrow(out)) return(data.table())
    keep_cols <- c("fint", "idema", "ta", "tamax", "tamin", "hr", "prec", "vv", "pres")
    missing <- setdiff(keep_cols, names(out))
    if (length(missing)) for (mc in missing) out[, (mc) := NA]
    out[, ..keep_cols]
  }, error = function(e) {
    cat("Station", station, "attempt", attempt, "failed:", conditionMessage(e), "\n")
    if (attempt >= 5L) return(data.table())
    cat("  Rotating API key and retrying after 6s...\n")
    rotate_api_key()
    handle_setheaders(handle, "api_key" = get_current_api_key())
    Sys.sleep(6)
    fetch_station_latest(station, attempt + 1L)
  })
}

latest_list <- lapply(barcelona_stations, fetch_station_latest)
latest <- rbindlist(latest_list, fill = TRUE)

if (!nrow(latest)) {
  cat("API returned no Barcelona station observations at this time.\n")
  quit(status = 0)
}

# Attach mapping metadata
latest <- merge(
  latest,
  unique(station_map_bcn[, .(idema = INDICATIVO, municipio_natcode = NATCODE, municipio_name = NAMEUNIT)]),
  by = "idema",
  all.x = TRUE
)

latest_long <- melt(
  latest,
  id.vars = c("fint", "idema", "municipio_natcode", "municipio_name"),
  variable.name = "measure",
  value.name = "value",
  variable.factor = FALSE
)[!is.na(value)]

latest_long[, fint := as_datetime(fint)]

ensure_columns <- function(dt) {
  if (!"municipio_natcode" %in% names(dt)) dt[, municipio_natcode := NA_character_]
  if (!"municipio_name" %in% names(dt)) dt[, municipio_name := NA_character_]
  if (!"measure" %in% names(dt)) dt[, measure := NA_character_]
  if (!"value" %in% names(dt)) dt[, value := NA_real_]
  dt
}

persist_latest <- function(new_rows) {
  new_rows <- ensure_columns(copy(new_rows))
  acquire_file_lock(lock_path)
  on.exit(release_file_lock(lock_path), add = TRUE)
  existing <- if (file.exists(output_path)) fread(output_path) else data.table()
  if (nrow(existing)) {
    if (!inherits(existing$fint, "POSIXct")) {
      existing[, fint := as_datetime(fint)]
    }
    existing <- ensure_columns(existing)
  }
  combined <- rbindlist(list(new_rows, existing), fill = TRUE)
  setorder(combined, -fint, idema, measure)
  combined <- unique(combined, by = c("fint", "idema", "measure"))
  fwrite(combined, output_path)
  combined
}

combined <- persist_latest(latest_long)

cat("Barcelona hourly dataset updated:", output_path, "\n")
cat("New rows added:", nrow(latest_long), " | Total rows:", nrow(combined), "\n")
