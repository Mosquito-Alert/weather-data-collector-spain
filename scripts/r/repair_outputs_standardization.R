#!/usr/bin/env Rscript

# Repairs the three output datasets in data/output by applying
# the shared standardization utilities. Creates timestamped backups.

suppressPackageStartupMessages({
  library(data.table)
  source("scripts/r/standardize_vars.R")
})

out_dir <- "data/output"
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

safe_fix <- function(file, standardize_fn) {
  path <- file.path(out_dir, file)
  if (!file.exists(path)) {
    cat("⚠️  Missing:", file, "\n")
    return(invisible(NULL))
  }
  cat("\n=== Repairing:", file, "===\n")
  dt <- tryCatch(fread(path), error = function(e) fread(path, encoding = "UTF-8"))
  cat("Loaded:", nrow(dt), "rows,", ncol(dt), "cols\n")
  backup <- paste0(path, ".backup_", ts)
  file.copy(path, backup)
  cat("Backup:", basename(backup), "\n")
  fixed <- standardize_fn(dt)
  # verify no numbered columns
  if (length(grep("\\\\.[0-9]+$", colnames(fixed))) > 0) {
    cat("❌ Numbered columns remain:", paste(grep("\\\\.[0-9]+$", colnames(fixed), value = TRUE), collapse=", "), "\n")
  } else {
    cat("✅ No numbered columns\n")
  }
  fwrite(fixed, path)
  cat("Saved:", file, "with", ncol(fixed), "columns\n")
}

safe_fix("daily_station_historical.csv", standardize_station_df)
safe_fix("daily_municipal_extended.csv", standardize_municipal_df)
safe_fix("hourly_station_ongoing.csv", standardize_hourly_df)

cat("\nAll done.\n")
