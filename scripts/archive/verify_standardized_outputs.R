#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

cat("=== Verify standardized outputs ===\n")

out_dir <- "data/output"

expect_station <- c(
  "date","station_id","station_name","province","altitude",
  "temp_mean","temp_max","temp_min","precipitation",
  "humidity_mean","humidity_max","humidity_min",
  "wind_speed","wind_direction","wind_gust",
  "pressure_max","pressure_min","solar_hours",
  "time_temp_min","time_temp_max","time_humidity_max","time_humidity_min",
  "time_pressure_max","time_pressure_min","time_wind_gust"
)

expect_muni <- c(
  "municipality_id","municipality_name","province","date",
  "temp_mean","temp_max","temp_min","humidity_mean","humidity_max","humidity_min","wind_speed",
  "forecast_issued_at","data_source","data_priority",
  "n_stations","collection_timestamp","processing_timestamp",
  "qc_temp_range","qc_temp_realistic"
)

expect_hourly <- c("station_id","datetime","date","variable_type","value")

errors <- 0

check_file <- function(file, expected, forbid_synonyms = character()) {
  path <- file.path(out_dir, file)
  if (!file.exists(path)) {
    cat("❌ Missing:", file, "\n")
    return(1)
  }
  dt <- fread(path, nrows = 100)
  cols <- names(dt)
  # No numbered suffixes
  numbered <- grep("\\\.[0-9]+$", cols, value = TRUE)
  if (length(numbered) > 0) {
    cat("❌ Numbered columns in", file, ":", paste(numbered, collapse=", "), "\n")
    n_err <- 1
  } else { n_err <- 0 }
  # Expected present
  missing <- setdiff(expected, cols)
  if (length(missing) > 0) {
    cat("❌ Missing expected columns in", file, ":", paste(missing, collapse=", "), "\n")
    n_err <- 1
  }
  # Synonyms absent
  syn_present <- intersect(forbid_synonyms, cols)
  if (length(syn_present) > 0) {
    cat("❌ Found synonym columns in", file, ":", paste(syn_present, collapse=", "), "\n")
    n_err <- 1
  }
  if (n_err == 0) {
    cat("✅", file, "OK (", length(cols), "columns)\n")
  }
  n_err
}

errors <- errors + check_file(
  "daily_station_historical.csv", expect_station,
  forbid_synonyms = c("fecha","indicativo","idema","tmed","tmax","tmin","prec","hrMedia","hrMax","hrMin","velmedia","dir","racha","presMax","presMin","sol","horatmin","horatmax","horaHrMax","horaHrMin","horaPresMax","horaPresMin","horaracha","measure","value")
)

errors <- errors + check_file(
  "daily_municipal_extended.csv", expect_muni,
  forbid_synonyms = c("municipio","municipio_id","municipio_code","municipio_nombre","tmax_municipal","tmin_municipal","velmedia_municipal","source","priority")
)

errors <- errors + check_file(
  "hourly_station_ongoing.csv", expect_hourly,
  forbid_synonyms = c("idema","fint","measure")
)

if (errors > 0) {
  cat("\n❌ Verification failed with", errors, "issue(s).\n")
  quit(status = 1)
} else {
  cat("\n✅ All outputs verified.\n")
}
