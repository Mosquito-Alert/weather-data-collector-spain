#!/usr/bin/env Rscript

# PROPER fix for the actual duplicate columns in the datasets
# This will examine content and consolidate intelligently

library(data.table)

cat("=== PROPER DUPLICATE COLUMN CONSOLIDATION ===\n")

# Fix Daily Station Historical Dataset
cat("\n--- FIXING DAILY STATION HISTORICAL ---\n")

# Load with fread to see the actual structure
station_data <- fread("data/output/daily_station_historical.csv")
station_cols <- colnames(station_data)

cat("Original columns:", length(station_cols), "\n")

# Examine the duplicate station_id columns
station_id_cols <- grep("station_id", station_cols, value = TRUE)
cat("Station ID columns:", paste(station_id_cols, collapse = ", "), "\n")

if (length(station_id_cols) > 1) {
  for (col in station_id_cols) {
    non_na_count <- sum(!is.na(station_data[[col]]))
    sample_vals <- unique(station_data[[col]][!is.na(station_data[[col]])])[1:3]
    cat("  ", col, ": ", non_na_count, " non-NA values, sample: ", paste(sample_vals, collapse=", "), "\n")
  }
}

# Examine duplicate date columns
date_cols <- grep("date", station_cols, value = TRUE) 
cat("Date columns:", paste(date_cols, collapse = ", "), "\n")

if (length(date_cols) > 1) {
  for (col in date_cols) {
    non_na_count <- sum(!is.na(station_data[[col]]))
    sample_vals <- unique(station_data[[col]][!is.na(station_data[[col]])])[1:3]
    cat("  ", col, ": ", non_na_count, " non-NA values, sample: ", paste(sample_vals, collapse=", "), "\n")
  }
}

# Create clean dataset with only the columns we need
# Define the correct standard columns according to docs
standard_station_cols <- c(
  "date", "station_id", "station_name", "province", "altitude",
  "temp_mean", "temp_max", "temp_min", "precipitation", 
  "humidity_mean", "humidity_max", "humidity_min",
  "wind_speed", "wind_direction", "wind_gust",
  "pressure_max", "pressure_min", "solar_hours",
  "time_temp_min", "time_temp_max", "time_humidity_max", "time_humidity_min",
  "time_pressure_max", "time_pressure_min", "time_wind_gust",
  "collection_timestamp", "processing_timestamp",
  "qc_temp_range", "qc_temp_realistic", "qc_prec_realistic", 
  "data_source"
)

# Build clean dataset
clean_station <- data.frame(stringsAsFactors = FALSE)

for (col in standard_station_cols) {
  if (col %in% station_cols) {
    # Use the column as-is
    clean_station[[col]] <- station_data[[col]]
    cat("  Kept:", col, "\n")
  } else {
    # Look for alternative names
    if (col == "station_id" && "station_id.1" %in% station_cols) {
      # Choose the best station_id column
      if (sum(!is.na(station_data[["station_id"]])) >= sum(!is.na(station_data[["station_id.1"]]))) {
        clean_station[[col]] <- station_data[["station_id"]]
      } else {
        clean_station[[col]] <- station_data[["station_id.1"]]
      }
      cat("  Consolidated station_id\n")
    } else if (col == "date" && "date.1" %in% station_cols) {
      # Choose the best date column
      if (sum(!is.na(station_data[["date"]])) >= sum(!is.na(station_data[["date.1"]]))) {
        clean_station[[col]] <- station_data[["date"]]
      } else {
        clean_station[[col]] <- station_data[["date.1"]]
      }
      cat("  Consolidated date\n")
    } else {
      cat("  Missing:", col, "\n")
    }
  }
}

cat("Clean dataset: ", nrow(clean_station), " rows, ", ncol(clean_station), " columns\n")

# Create backup and save
backup_file <- paste0("data/output/daily_station_historical.csv.backup_proper_fix_", 
                     format(Sys.time(), "%Y%m%d_%H%M%S"))
file.copy("data/output/daily_station_historical.csv", backup_file)
cat("Backup created:", basename(backup_file), "\n")

# Write clean version
fwrite(clean_station, "data/output/daily_station_historical.csv")
cat("✅ Daily station dataset properly cleaned\n")

# Verify result
final_data <- fread("data/output/daily_station_historical.csv", nrows = 1)
final_cols <- colnames(final_data)
numbered_final <- grep("\\.[0-9]+$", final_cols, value = TRUE)

cat("\nFinal verification:\n")
cat("  Columns:", length(final_cols), "\n")
if (length(numbered_final) > 0) {
  cat("  ❌ Still has numbered columns:", paste(numbered_final, collapse = ", "), "\n")
} else {
  cat("  ✅ No numbered columns\n")
}

cat("\n🎯 PROPER CONSOLIDATION COMPLETE\n")
