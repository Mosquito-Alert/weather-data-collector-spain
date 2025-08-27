#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  source("scripts/r/standardize_vars.R")
})

cat("=== Municipal aggregation (archived) + standardization ===\n")

# Run archived municipal aggregator to produce municipal_aggregated_*.csv(.gz)
latest <- function(pattern) {
  files <- list.files("data/output", pattern = pattern, full.names = TRUE)
  if (length(files) == 0) return(NULL)
  files[which.max(file.mtime(files))]
}

arch_file <- latest("^municipal_aggregated_.*\\.(csv|csv.gz)$")
if (is.null(arch_file)) {
  cat("Running archived municipal aggregator...\n")
  sys <- system2("Rscript", args = c("scripts/archive/aggregate_municipal_data_hybrid.R"), stdout = TRUE, stderr = TRUE)
  cat(paste(sys, collapse = "\n"), "\n")
  arch_file <- latest("^municipal_aggregated_.*\\.(csv|csv.gz)$")
}
if (is.null(arch_file)) stop("Municipal aggregated file not found.")

cat("Using:", arch_file, "\n")
muni <- fread(arch_file)

# Standardize to documented names
muni_std <- standardize_municipal_df(muni)

out_final <- "data/output/daily_municipal_extended.csv"
fwrite(muni_std, out_final)
cat("Saved:", out_final, "\n")

cat("Done.\n")
