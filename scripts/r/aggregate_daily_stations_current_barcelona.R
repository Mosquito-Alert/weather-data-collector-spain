#!/usr/bin/env Rscript

# aggregate_daily_stations_current_barcelona.R
# --------------------------------------------
# Purpose: Aggregate the Barcelona-only hourly observation feed into daily
# metrics for the recent window, mirroring the nationwide workflow.

rm(list = ls())

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
})

input_path <- "data/output/hourly_station_ongoing_barcelona.csv.gz"
output_path <- "data/output/daily_station_current_barcelona.csv.gz"
station_map_path <- "data/input/station_point_municipaities_table.csv"
lock_path <- paste0(output_path, ".lock")
LOCK_TIMEOUT_SECONDS <- 600
LOCK_SLEEP_SECONDS <- 1

if (!file.exists(input_path)) {
  stop("Hourly Barcelona dataset not found at ", input_path, ". Run get_latest_data_barcelona.R first.")
}

hourly <- fread(input_path, showProgress = FALSE)
if (!nrow(hourly)) {
  cat("Hourly Barcelona dataset empty; nothing to aggregate.\n")
  quit(status = 0)
}

required_cols <- c("fint", "idema", "measure", "value")
missing_cols <- setdiff(required_cols, names(hourly))
if (length(missing_cols)) {
  stop("Hourly dataset is missing columns: ", paste(missing_cols, collapse = ", "))
}

hourly[, fint := as_datetime(fint)]
hourly[, date := as_date(fint)]

if (!"municipio_natcode" %in% names(hourly)) hourly[, municipio_natcode := NA_character_]
if (!"municipio_name" %in% names(hourly)) hourly[, municipio_name := NA_character_]

if (file.exists(station_map_path)) {
  station_map <- fread(
    station_map_path,
    colClasses = list(character = c("INDICATIVO", "NATCODE", "NAMEUNIT"))
  )
  station_map <- unique(station_map[, .(idema = INDICATIVO, municipio_natcode = NATCODE, municipio_name = NAMEUNIT)])
  hourly <- merge(hourly, station_map, by = "idema", all.x = TRUE)
  if ("i.municipio_natcode" %in% names(hourly)) {
    hourly[is.na(municipio_natcode) & !is.na(`i.municipio_natcode`), municipio_natcode := `i.municipio_natcode`]
    hourly[, `i.municipio_natcode` := NULL]
  }
  if ("i.municipio_name" %in% names(hourly)) {
    hourly[is.na(municipio_name) & !is.na(`i.municipio_name`), municipio_name := `i.municipio_name`]
    hourly[, `i.municipio_name` := NULL]
  }
} else {
  hourly[, `:=`(municipio_natcode = NA_character_, municipio_name = NA_character_)]
}

# Pivot to wide per timestamp for easier aggregation
wide <- dcast(
  hourly,
  idema + municipio_natcode + municipio_name + fint + date ~ measure,
  value.var = "value",
  fun.aggregate = function(x) suppressWarnings(mean(as.numeric(x), na.rm = TRUE)),
  fill = NA_real_
)

# Ensure expected measurement columns exist even if API omits them
expected_measures <- c("ta", "tamax", "tamin", "hr", "prec", "vv", "pres")
missing_measures <- setdiff(expected_measures, names(wide))
if (length(missing_measures)) {
  for (mc in missing_measures) {
    wide[, (mc) := NA_real_]
  }
}

if (!nrow(wide)) {
  cat("Nothing to aggregate after reshaping; exiting.\n")
  quit(status = 0)
}

# Aggregation helpers
agg_na <- function(x, fun) {
  res <- suppressWarnings(fun(x, na.rm = TRUE))
  if (is.infinite(res)) NA_real_ else res
}

# Daily summaries per station
summary_daily <- wide[, .(
  tmed = agg_na(ta, mean),
  tmax = agg_na(c(tamax, ta), max),
  tmin = agg_na(c(tamin, ta), min),
  hrMedia = agg_na(hr, mean),
  hrMax = agg_na(hr, max),
  hrMin = agg_na(hr, min),
  prec = agg_na(prec, sum),
  velmedia = agg_na(vv, mean),
  presMax = agg_na(pres, max),
  presMin = agg_na(pres, min),
  n_obs = .N
), by = .(
  fecha = date,
  indicativo = idema,
  idema,
  municipio_natcode,
  municipio_name
)]

setorder(summary_daily, fecha, indicativo)

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

load_existing_daily <- function(path) {
  if (!file.exists(path)) return(data.table())
  dt <- fread(path, showProgress = FALSE)
  if ("fecha" %in% names(dt)) dt[, fecha := as_date(fecha)]
  dt
}

persist_daily <- function(new_rows) {
  acquire_file_lock(lock_path)
  on.exit(release_file_lock(lock_path), add = TRUE)
  existing <- load_existing_daily(output_path)
  combined <- rbindlist(list(existing, new_rows), fill = TRUE)
  setorder(combined, fecha, indicativo)
  combined <- unique(combined, by = c("fecha", "indicativo"))
  fwrite(combined, output_path)
  combined
}

combined <- persist_daily(summary_daily)

cat("Barcelona current-daily dataset updated:", output_path, "\n")
cat("Rows added this run:", nrow(summary_daily), " | Total rows stored:", nrow(combined), "\n")
cat("Date range:", as.character(min(combined$fecha)), "to", as.character(max(combined$fecha)), "\n")
