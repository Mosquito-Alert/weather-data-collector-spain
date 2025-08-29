#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  source("scripts/r/standardize_vars.R")
})

cat("=== Hourly ongoing standardization ===\n")

hourly_paths <- c(
  "data/output/hourly_station_ongoing.csv",
  "data/output/hourly_station_ongoing.csv.gz"
)
path <- hourly_paths[file.exists(hourly_paths)][1]
if (is.na(path)) {
  cat("No hourly ongoing file found; running archived hourly collector...\n")
  sys <- tryCatch(
    system2("Rscript", args = c("scripts/archive/get_latest_data.R"), stdout = TRUE, stderr = TRUE),
    error = function(e) e
  )
  if (inherits(sys, "error")) {
    stop("Failed to run archived hourly collector: ", sys$message)
  } else {
    cat(paste(sys, collapse = "\n"), "\n")
  }
  path <- hourly_paths[file.exists(hourly_paths)][1]
  if (is.na(path)) stop("Hourly collector did not produce an output file.")
}

# Load and check content
dt <- fread(path, showProgress = FALSE)
if (nrow(dt) == 0) {
  cat("Hourly file is empty; trying archived hourly collector...\n")
  sys <- tryCatch(
    system2("Rscript", args = c("scripts/archive/get_latest_data.R"), stdout = TRUE, stderr = TRUE),
    error = function(e) e
  )
  if (!inherits(sys, "error")) cat(paste(sys, collapse = "\n"), "\n")
  # Prefer gz if produced
  path_gz <- "data/output/hourly_station_ongoing.csv.gz"
  if (file.exists(path_gz)) {
    path <- path_gz
  }
  dt <- fread(path, showProgress = FALSE)
}
dt_std <- standardize_hourly_df(dt)

out_final_csv <- "data/output/hourly_station_ongoing.csv"
out_final_gz  <- "data/output/hourly_station_ongoing.csv.gz"
fwrite(dt_std, out_final_csv)
fwrite(dt_std, out_final_gz)
cat("Saved:", out_final_csv, "and", out_final_gz, "\n")

cat("Done.\n")
