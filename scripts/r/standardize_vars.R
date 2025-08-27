#!/usr/bin/env Rscript

# Shared variable standardization utilities
# Source this file from aggregation/collection scripts to ensure consistent naming

suppressPackageStartupMessages({
  library(dplyr)
  library(data.table)
})

# Mapping dictionaries derived from docs/variable_standardization.md
std_map_station <- c(
  # identifiers
  fecha = "date",
  indicativo = "station_id",
  idema = "station_id",
  nombre = "station_name",
  provincia = "province",
  altitud = "altitude",
  # weather
  tmed = "temp_mean",
  tmax = "temp_max",
  tmin = "temp_min",
  prec = "precipitation",
  hrMedia = "humidity_mean",
  hrMax = "humidity_max",
  hrMin = "humidity_min",
  velmedia = "wind_speed",
  dir = "wind_direction",
  racha = "wind_gust",
  presMax = "pressure_max",
  presMin = "pressure_min",
  sol = "solar_hours",
  # timing
  horatmin = "time_temp_min",
  horatmax = "time_temp_max",
  horaHrMax = "time_humidity_max",
  horaHrMin = "time_humidity_min",
  horaPresMax = "time_pressure_max",
  horaPresMin = "time_pressure_min",
  horaracha = "time_wind_gust"
)

std_map_municipal <- c(
  # identifiers
  municipio_id = "municipality_id",
  municipio = "municipality_id",
  municipio_code = "municipality_id",
  municipio_nombre = "municipality_name",
  provincia = "province",
  fecha = "date",
  elaborado = "forecast_issued_at",
  # weather
  temp_avg = "temp_mean",
  tmed_municipal = "temp_mean",
  temp_max = "temp_max",
  tmax_municipal = "temp_max",
  temp_min = "temp_min",
  tmin_municipal = "temp_min",
  hrMedia_municipal = "humidity_mean",
  humid_max = "humidity_max",
  humid_min = "humidity_min",
  wind_speed = "wind_speed",
  velmedia_municipal = "wind_speed",
  # metadata
  source = "data_source",
  data_source = "data_source",
  priority = "data_priority",
  data_priority = "data_priority"
)

std_map_hourly <- c(
  idema = "station_id",
  fint = "datetime",
  date = "date",
  measure = "variable_type",
  value = "value"
)

# Helper: rename/coalesce with mapping.
# If both old and new exist, coalesce values into the new and drop the old.
# If only old exists, rename to new.
rename_with_map <- function(dt, mapping) {
  dt <- as.data.table(dt)
  cols <- colnames(dt)
  for (old in names(mapping)) {
    if (!(old %in% cols)) next
    new <- unname(mapping[[old]])
    if (!(new %in% cols)) {
      # simple rename
      setnames(dt, old, new)
      cols[cols == old] <- new
    } else if (old != new) {
      # coalesce into new then drop old
      na_mask <- is.na(dt[[new]]) | dt[[new]] == ""
      if (old %in% names(dt)) {
        dt[[new]][na_mask] <- dt[[old]][na_mask]
        dt[, (old) := NULL]
        cols <- setdiff(cols, old)
      }
    }
  }
  invisible(dt)
}

# Helper: consolidate duplicates for a base name (e.g., station_id, station_id.1, station_id_alt)
consolidate_preferred <- function(dt, base, prefer = NULL) {
  cols <- colnames(dt)
  matches <- cols[grepl(paste0("^", base, "($|\\\\.)"), cols)]
  if (length(matches) <= 1) return(dt)
  # choose preferred column: by priority list then by most non-NA
  preferred <- intersect(c(prefer, base, paste0(base, ".1"), paste0(base, ".2")), matches)
  if (length(preferred) == 0) preferred <- matches
  # pick by most non-NA
  best <- preferred[1]
  best_non_na <- sum(!is.na(dt[[best]]))
  for (m in matches) {
    nn <- sum(!is.na(dt[[m]]))
    if (nn > best_non_na) { best <- m; best_non_na <- nn }
  }
  # create final column
  if (!(base %in% cols)) dt[, (base) := NA]
  # fill from best, then from others for NAs
  dt[[base]] <- dt[[best]]
  for (m in setdiff(matches, c(best, base))) {
    na_mask <- is.na(dt[[base]]) | dt[[base]] == ""
    dt[[base]][na_mask] <- dt[[m]][na_mask]
  }
  # drop all others except the base
  drop_cols <- setdiff(matches, base)
  dt[, (drop_cols) := NULL]
  invisible(dt)
}

# Helper: remove numbered suffix duplicates in general (keep first occurrence)
drop_numbered_dupes <- function(dt) {
  cols <- colnames(dt)
  keep <- !duplicated(gsub("\\\\.[0-9]+$", "", cols))
  dt <- dt[, ..keep]
  setnames(dt, gsub("\\\\.[0-9]+$", "", colnames(dt)))
  dt
}

# Helper: ensure a canonical column exists and drop/rename a synonym as needed
fix_synonym <- function(dt, synonym, canonical) {
  syn_present <- synonym %in% names(dt)
  can_present <- canonical %in% names(dt)
  if (!syn_present) return(dt)
  if (!can_present) {
    # simple rename if canonical missing
    setnames(dt, synonym, canonical)
  } else {
    # coalesce values then drop synonym
    na_mask <- is.na(dt[[canonical]]) | dt[[canonical]] == ""
    dt[[canonical]][na_mask] <- dt[[synonym]][na_mask]
    dt[, (synonym) := NULL]
  }
  dt
}

# Standardizers for each dataset shape
standardize_station_df <- function(dt) {
  dt <- as.data.table(dt)
  rename_with_map(dt, std_map_station)
  # consolidate critical IDs and date
  dt <- consolidate_preferred(dt, "station_id", prefer = c("station_id", "idema"))
  dt <- consolidate_preferred(dt, "date", prefer = c("date", "fecha"))
  # Drop long-form columns that don't belong in the daily station wide dataset
  drop_if_present <- intersect(c("measure","value"), names(dt))
  if (length(drop_if_present)) dt[, (drop_if_present) := NULL]
  # remove numbered suffixes and keep first
  dt <- drop_numbered_dupes(dt)
  # order columns (standard first)
  standard_order <- c(
    "date","station_id","station_name","province","altitude",
    "temp_mean","temp_max","temp_min","precipitation",
    "humidity_mean","humidity_max","humidity_min",
    "wind_speed","wind_direction","wind_gust",
    "pressure_max","pressure_min","solar_hours",
    "time_temp_min","time_temp_max","time_humidity_max","time_humidity_min",
    "time_pressure_max","time_pressure_min","time_wind_gust"
  )
  extra <- setdiff(colnames(dt), standard_order)
  setcolorder(dt, c(intersect(standard_order, colnames(dt)), extra))
  dt
}

standardize_municipal_df <- function(dt) {
  dt <- as.data.table(dt)
  rename_with_map(dt, std_map_municipal)
  dt <- consolidate_preferred(dt, "municipality_id", prefer = c("municipality_id","municipio","municipio_code"))
  dt <- consolidate_preferred(dt, "date", prefer = c("date","fecha"))
  # Enforce canonical names for known synonyms (belt-and-suspenders)
  dt <- fix_synonym(dt, "tmax_municipal", "temp_max")
  dt <- fix_synonym(dt, "tmin_municipal", "temp_min")
  dt <- fix_synonym(dt, "velmedia_municipal", "wind_speed")
  dt <- fix_synonym(dt, "source", "data_source")
  dt <- fix_synonym(dt, "priority", "data_priority")
  
  # Normalize municipality_id to canonical 5-digit code (CUMUN/INE-like)
  # - Preserve original in municipality_id_raw for traceability
  # - If length < 5: left-pad with zeros (e.g., 1030 -> 01030)
  # - If length > 5 (e.g., 11-digit composites): keep first 5 digits
  if ("municipality_id" %in% names(dt)) {
    if (!"municipality_id_raw" %in% names(dt)) dt[, municipality_id_raw := as.character(municipality_id)]
    dt[, municipality_id := as.character(municipality_id)]
    dt[, municipality_id := gsub("[^0-9]", "", municipality_id)]
    suppressWarnings({
      dt[nchar(municipality_id) == 0, municipality_id := NA_character_]
      # Left-pad shorter than 5
      dt[nchar(municipality_id) > 0 & nchar(municipality_id) < 5, municipality_id := sprintf("%05d", as.integer(municipality_id))]
      # If exactly 5, keep as-is (ensure zero-padded characters preserved)
      # If longer than 5, truncate to first 5 digits
      dt[nchar(municipality_id) > 5, municipality_id := substr(municipality_id, 1L, 5L)]
    })
  }
  dt <- drop_numbered_dupes(dt)
  standard_order <- c(
    "municipality_id","municipality_name","province","date",
    "temp_mean","temp_max","temp_min","humidity_mean","humidity_max","humidity_min","wind_speed",
    "forecast_issued_at","data_source","data_priority",
    "n_stations","collection_timestamp","processing_timestamp",
    "qc_temp_range","qc_temp_realistic","municipality_id_raw"
  )
  extra <- setdiff(colnames(dt), standard_order)
  setcolorder(dt, c(intersect(standard_order, colnames(dt)), extra))
  dt
}

standardize_hourly_df <- function(dt) {
  dt <- as.data.table(dt)
  rename_with_map(dt, std_map_hourly)
  # Fallback: enforce direct renames if still in raw schema
  if ("idema" %in% names(dt) && !("station_id" %in% names(dt))) setnames(dt, "idema", "station_id")
  if ("fint" %in% names(dt) && !("datetime" %in% names(dt))) setnames(dt, "fint", "datetime")
  if ("measure" %in% names(dt) && !("variable_type" %in% names(dt))) setnames(dt, "measure", "variable_type")
  dt <- consolidate_preferred(dt, "station_id", prefer = c("station_id","idema"))
  dt <- consolidate_preferred(dt, "datetime", prefer = c("datetime","fint"))
  dt <- consolidate_preferred(dt, "date", prefer = c("date"))
  if (!"date" %in% names(dt) && "datetime" %in% names(dt)) {
    suppressWarnings({
      dt[, date := as.Date(datetime)]
    })
  }
  dt <- drop_numbered_dupes(dt)
  standard_order <- c("station_id","datetime","date","variable_type","value")
  extra <- setdiff(colnames(dt), standard_order)
  setcolorder(dt, c(intersect(standard_order, colnames(dt)), extra))
  dt
}

# If executed directly, do nothing. This file is meant to be sourced.
if (identical(environment(), globalenv()) && !interactive()) {
  message("standardize_vars.R loaded. Source this in your scripts.")
}
