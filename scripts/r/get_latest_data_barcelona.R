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

handle <- new_handle()
handle_setheaders(handle, "api_key" = get_current_api_key())

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
    data_url <- fromJSON(rawToChar(req$content))$datos
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

if (file.exists(output_path)) {
  existing <- fread(output_path)
  combined <- rbindlist(list(latest_long, existing), fill = TRUE)
  setorder(combined, -as.numeric(fint))
  combined <- unique(combined, by = c("fint", "idema", "measure"))
} else {
  combined <- latest_long[order(-as.numeric(fint))]
}

fwrite(combined, output_path)

cat("Barcelona hourly dataset updated:", output_path, "\n")
cat("New rows added:", nrow(latest_long), " | Total rows:", nrow(combined), "\n")
