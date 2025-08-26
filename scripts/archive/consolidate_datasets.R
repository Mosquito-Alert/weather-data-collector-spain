#!/usr/bin/env Rscript

# consolidate_datasets.R
# Safely consolidates all fragmented data files into three final datasets
# Ensures no data loss and removes duplicates

rm(list=ls())
library(tidyverse)
library(lubridate)
library(data.table)

cat("=====================================\n")
cat("DATA CONSOLIDATION - THREE FILE STRATEGY\n")
cat("=====================================\n")
cat("Consolidating fragmented data into:\n")
cat("1. daily_station_historical.csv\n")
cat("2. daily_municipal_extended.csv\n") 
cat("3. hourly_station_ongoing.csv\n\n")

# Create final output directory if needed
dir.create("data/output/final", showWarnings = FALSE)

# ===== 1. CONSOLIDATE STATION DAILY DATA =====
cat("=== CONSOLIDATING STATION DAILY DATA ===\n")

# Find all station daily files
station_files = c(
  list.files("data/output", pattern = "station_daily_data_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "daily_station_aggregated.*\\.csv$", full.names = TRUE),
  "data/output/daily_station_historical.csv.gz"
)
station_files = station_files[file.exists(station_files)]

cat("Found station files:\n")
for(f in station_files) cat("  -", basename(f), "\n")

all_station_data = data.table()

for(file in station_files) {
  cat("Loading:", basename(file), "...")
  
  if(grepl("\\.gz$", file)) {
    temp_data = fread(file)
  } else {
    temp_data = fread(file)
  }
  
  # Standardize column names
  if("indicativo" %in% names(temp_data) && !"idema" %in% names(temp_data)) {
    temp_data$idema = temp_data$indicativo
  }
  if("fecha" %in% names(temp_data) && !"date" %in% names(temp_data)) {
    temp_data$date = as.Date(temp_data$fecha)
  }
  if(!"date" %in% names(temp_data) && "fecha" %in% names(temp_data)) {
    temp_data$date = as.Date(temp_data$fecha)  
  }
  
  # Ensure date is Date type
  if("date" %in% names(temp_data)) {
    temp_data$date = as.Date(temp_data$date)
  }
  
  all_station_data = rbind(all_station_data, temp_data, fill = TRUE)
  cat(" loaded", nrow(temp_data), "rows\n")
}

# Remove duplicates based on station + date
if(nrow(all_station_data) > 0) {
  cat("Removing duplicates...\n")
  original_rows = nrow(all_station_data)
  
  # Use idema + date as key for deduplication
  if("idema" %in% names(all_station_data) && "date" %in% names(all_station_data)) {
    all_station_data = all_station_data[!duplicated(all_station_data[, .(idema, date)]), ]
  }
  
  duplicates_removed = original_rows - nrow(all_station_data)
  cat("Removed", duplicates_removed, "duplicate rows\n")
  
  # Sort by date and station
  all_station_data = all_station_data[order(date, idema)]
  
  cat("Final station dataset:\n")
  cat("  Rows:", nrow(all_station_data), "\n")
  cat("  Unique stations:", length(unique(all_station_data$idema)), "\n")
  cat("  Date range:", as.character(min(all_station_data$date, na.rm=TRUE)), 
      "to", as.character(max(all_station_data$date, na.rm=TRUE)), "\n")
  
  # Save consolidated file
  output_file = "data/output/daily_station_historical.csv"
  fwrite(all_station_data, output_file)
  cat("✅ Saved to:", output_file, "\n")
  
  # Compressed version
  fwrite(all_station_data, paste0(output_file, ".gz"))
  cat("✅ Compressed version saved\n\n")
} else {
  cat("⚠️  No station data found\n\n")
}

# ===== 2. CONSOLIDATE MUNICIPAL DATA =====
cat("=== CONSOLIDATING MUNICIPAL DATA ===\n")

# Find all municipal files
municipal_files = c(
  list.files("data/output", pattern = "municipal_forecasts_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "municipal_aggregated.*\\.csv$", full.names = TRUE),
  "data/output/daily_municipal_extended.csv",
  "data/output/daily_municipal_extended.csv.gz"
)
municipal_files = municipal_files[file.exists(municipal_files)]

cat("Found municipal files:\n")
for(f in municipal_files) cat("  -", basename(f), "\n")

all_municipal_data = data.table()

for(file in municipal_files) {
  cat("Loading:", basename(file), "...")
  
  if(grepl("\\.gz$", file)) {
    temp_data = fread(file)
  } else {
    temp_data = fread(file)
  }
  
  # Standardize date column
  if("fecha" %in% names(temp_data) && !"date" %in% names(temp_data)) {
    temp_data$date = as.Date(temp_data$fecha)
  }
  if("date" %in% names(temp_data)) {
    temp_data$date = as.Date(temp_data$date)
  }
  
  all_municipal_data = rbind(all_municipal_data, temp_data, fill = TRUE)
  cat(" loaded", nrow(temp_data), "rows\n")
}

# Remove duplicates
if(nrow(all_municipal_data) > 0) {
  cat("Removing duplicates...\n")
  original_rows = nrow(all_municipal_data)
  
  # Use appropriate key for deduplication
  if("id" %in% names(all_municipal_data) && "date" %in% names(all_municipal_data)) {
    all_municipal_data = all_municipal_data[!duplicated(all_municipal_data[, .(id, date)]), ]
  } else if("municipio" %in% names(all_municipal_data) && "fecha" %in% names(all_municipal_data)) {
    all_municipal_data = all_municipal_data[!duplicated(all_municipal_data[, .(municipio, fecha)]), ]
  }
  
  duplicates_removed = original_rows - nrow(all_municipal_data)
  cat("Removed", duplicates_removed, "duplicate rows\n")
  
  cat("Final municipal dataset:\n")
  cat("  Rows:", nrow(all_municipal_data), "\n")
  if("id" %in% names(all_municipal_data)) {
    cat("  Unique municipalities:", length(unique(all_municipal_data$id)), "\n")
  }
  if("date" %in% names(all_municipal_data)) {
    cat("  Date range:", as.character(min(all_municipal_data$date, na.rm=TRUE)), 
        "to", as.character(max(all_municipal_data$date, na.rm=TRUE)), "\n")
  }
  
  # Save consolidated file
  output_file = "data/output/daily_municipal_extended.csv"
  fwrite(all_municipal_data, output_file)
  cat("✅ Saved to:", output_file, "\n")
  
  # Compressed version
  fwrite(all_municipal_data, paste0(output_file, ".gz"))
  cat("✅ Compressed version saved\n\n")
} else {
  cat("⚠️  No municipal data found\n\n")
}

# ===== 3. CONSOLIDATE HOURLY DATA =====
cat("=== CONSOLIDATING HOURLY DATA ===\n")

# Find hourly files
hourly_files = c(
  list.files("data/output", pattern = "latest_weather_.*\\.csv$", full.names = TRUE),
  "data/output/hourly_station_ongoing.csv.gz"
)
hourly_files = hourly_files[file.exists(hourly_files)]

cat("Found hourly files:\n")
for(f in hourly_files) cat("  -", basename(f), "\n")

all_hourly_data = data.table()

for(file in hourly_files) {
  cat("Loading:", basename(file), "...")
  
  if(grepl("\\.gz$", file)) {
    temp_data = fread(file)
  } else {
    temp_data = fread(file)
  }
  
  # Standardize datetime columns
  if("fhora" %in% names(temp_data)) {
    temp_data$datetime = as.POSIXct(temp_data$fhora)
  }
  
  all_hourly_data = rbind(all_hourly_data, temp_data, fill = TRUE)
  cat(" loaded", nrow(temp_data), "rows\n")
}

# Remove duplicates
if(nrow(all_hourly_data) > 0) {
  cat("Removing duplicates...\n")
  original_rows = nrow(all_hourly_data)
  
  # Use station + datetime for deduplication
  if("idema" %in% names(all_hourly_data) && "fhora" %in% names(all_hourly_data)) {
    all_hourly_data = all_hourly_data[!duplicated(all_hourly_data[, .(idema, fhora)]), ]
  } else if("idema" %in% names(all_hourly_data) && "datetime" %in% names(all_hourly_data)) {
    all_hourly_data = all_hourly_data[!duplicated(all_hourly_data[, .(idema, datetime)]), ]
  }
  
  duplicates_removed = original_rows - nrow(all_hourly_data)
  cat("Removed", duplicates_removed, "duplicate rows\n")
  
  cat("Final hourly dataset:\n")
  cat("  Rows:", nrow(all_hourly_data), "\n")
  cat("  Unique stations:", length(unique(all_hourly_data$idema)), "\n")
  if("fhora" %in% names(all_hourly_data)) {
    cat("  Datetime range:", as.character(min(all_hourly_data$fhora, na.rm=TRUE)), 
        "to", as.character(max(all_hourly_data$fhora, na.rm=TRUE)), "\n")
  }
  
  # Save consolidated file
  output_file = "data/output/hourly_station_ongoing.csv"
  fwrite(all_hourly_data, output_file)
  cat("✅ Saved to:", output_file, "\n")
  
  # Compressed version
  fwrite(all_hourly_data, paste0(output_file, ".gz"))
  cat("✅ Compressed version saved\n\n")
} else {
  cat("⚠️  No hourly data found\n\n")
}

# ===== CLEANUP =====
cat("=== CLEANUP OPTIONS ===\n")
cat("The following dated files can now be removed (consolidated data preserved):\n")

cleanup_files = c(
  list.files("data/output", pattern = "station_daily_data_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "daily_station_aggregated_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "municipal_forecasts_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "municipal_aggregated_.*\\.csv$", full.names = TRUE),
  list.files("data/output", pattern = "latest_weather_.*\\.csv$", full.names = TRUE)
)

for(f in cleanup_files) {
  if(file.exists(f)) {
    file_size = round(file.size(f) / 1024 / 1024, 2)
    cat("  ", basename(f), " (", file_size, " MB)\n")
  }
}

cat("\nTo clean up (AFTER verifying consolidated files), run:\n")
cat("Rscript -e \"file.remove(c(")
cat(paste0("'", cleanup_files, "'", collapse = ", "))
cat("))\"\n\n")

cat("=====================================\n")
cat("CONSOLIDATION COMPLETE\n")
cat("=====================================\n")
cat("Final three datasets:\n")
cat("1. data/output/daily_station_historical.csv(.gz)\n")
cat("2. data/output/daily_municipal_extended.csv(.gz)\n")
cat("3. data/output/hourly_station_ongoing.csv(.gz)\n")
cat("Original data backed up in: data/backup/\n")
