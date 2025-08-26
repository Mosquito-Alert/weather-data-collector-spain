#!/usr/bin/env Rscript

# Simple standardization script
library(dplyr, warn.conflicts = FALSE)

# Standardized variable mapping
standard_variables <- list(
  # Core identifiers
  "fecha" = "date",
  "indicativo" = "station_id", 
  "idema" = "station_id_alt",
  "nombre" = "station_name",
  "provincia" = "province",
  "altitud" = "altitude",
  
  # Temperature variables
  "tmed" = "temp_mean",
  "tmin" = "temp_min", 
  "tmax" = "temp_max",
  "horatmin" = "time_temp_min",
  "horatmax" = "time_temp_max",
  
  # Precipitation
  "prec" = "precipitation",
  
  # Wind variables
  "dir" = "wind_direction",
  "velmedia" = "wind_speed_mean",
  "racha" = "wind_gust_max",
  "horaracha" = "time_wind_gust_max",
  
  # Atmospheric pressure
  "presMax" = "pressure_max",
  "horaPresMax" = "time_pressure_max", 
  "presMin" = "pressure_min",
  "horaPresMin" = "time_pressure_min",
  
  # Humidity
  "hrMedia" = "humidity_mean",
  "hrMax" = "humidity_max",
  "horaHrMax" = "time_humidity_max",
  "hrMin" = "humidity_min", 
  "horaHrMin" = "time_humidity_min",
  
  # Solar radiation
  "sol" = "solar_radiation",
  
  # Quality control flags
  "temp_range_ok" = "qc_temp_range",
  "temp_realistic" = "qc_temp_realistic", 
  "prec_realistic" = "qc_prec_realistic",
  
  # Metadata
  "collected_at" = "collection_timestamp",
  "processed_at" = "processing_timestamp",
  "source" = "data_source",
  "n_observations" = "observation_count"
)

# Process each dataset
datasets <- c(
  "daily_station_historical.csv",
  "daily_municipal_extended.csv", 
  "hourly_station_ongoing.csv"
)

data_dir <- "/home/j.palmer/research/weather-data-collector-spain/data/output"

for (dataset_file in datasets) {
  file_path <- file.path(data_dir, dataset_file)
  
  if (!file.exists(file_path)) {
    cat("Warning: File", dataset_file, "not found\n")
    next
  }
  
  cat("Processing:", dataset_file, "\n")
  
  tryCatch({
    # Read the data
    data <- read.csv(file_path, stringsAsFactors = FALSE)
    
    cat("  Original dimensions:", nrow(data), "rows,", ncol(data), "columns\n")
    
    # Create backup
    backup_file <- paste0(file_path, ".backup_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy(file_path, backup_file)
    
    # Apply renames
    current_cols <- colnames(data)
    for (old_name in current_cols) {
      if (old_name %in% names(standard_variables)) {
        new_name <- standard_variables[[old_name]]
        # Only rename if new name doesn't already exist
        if (!new_name %in% colnames(data)) {
          colnames(data)[colnames(data) == old_name] <- new_name
          cat("    Renamed:", old_name, "->", new_name, "\n")
        }
      }
    }
    
    cat("  Final dimensions:", nrow(data), "rows,", ncol(data), "columns\n")
    
    # Write standardized version
    write.csv(data, file_path, row.names = FALSE)
    cat("  ✓ Successfully standardized", dataset_file, "\n\n")
    
  }, error = function(e) {
    cat("  ✗ Error processing", dataset_file, ":", e$message, "\n\n")
  })
}

cat("Variable standardization completed!\n")
