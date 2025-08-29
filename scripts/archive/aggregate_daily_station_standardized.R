#!/usr/bin/env Rscript

# Wrapper to run archived station aggregation then standardize output to final CSV

suppressPackageStartupMessages({
  library(data.table)
  source("scripts/r/standardize_vars.R")
})

cat("=== Station aggregation (archived) + standardization ===\n")

# Run archived aggregation if its output exists; otherwise, try to run the archived script
arch_out <- "data/output/daily_station_aggregated.csv.gz"
if (!file.exists(arch_out)) {
  cat("Running archived aggregator...\n")
  sys <- system2("Rscript", args = c("scripts/archive/aggregate_daily_station_data.R"), stdout = TRUE, stderr = TRUE)
  cat(paste(sys, collapse = "\n"), "\n")
}
if (!file.exists(arch_out)) {
  stop("Missing ", arch_out)
}

agg <- fread(arch_out)

# If the archived output is in long form (measure/value), pivot to wide with documented names
if (all(c("date","idema","measure","value") %in% names(agg))) {
  cat("Pivoting long -> wide...\n")
  wide <- dcast(agg, date + idema ~ measure, value.var = "value", fun.aggregate = mean, na.rm = TRUE)
  setnames(wide, "idema", "station_id")
  # Standardize column names per docs
  wide <- standardize_station_df(wide)
  # Save final
  out_final <- "data/output/daily_station_historical.csv"
  fwrite(wide, out_final)
  cat("Saved:", out_final, "\n")
} else {
  cat("Archived format not recognized; attempting direct standardization...\n")
  agg <- standardize_station_df(agg)
  out_final <- "data/output/daily_station_historical.csv"
  fwrite(agg, out_final)
  cat("Saved:", out_final, "\n")
}

cat("Done.\n")
