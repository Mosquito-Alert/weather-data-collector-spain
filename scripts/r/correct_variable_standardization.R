#!/usr/bin/env Rscript

# Correct variable standardization based on official documentation
# Uses the proper naming from docs/variable_standardization.md

library(dplyr, warn.conflicts = FALSE)

# Function to safely rename columns
safe_rename_columns <- function(data, variable_map) {
  current_cols <- colnames(data)
  rename_list <- list()
  
  for (old_name in current_cols) {
    if (old_name %in% names(variable_map)) {
      new_name <- variable_map[[old_name]]
      # Only rename if new name doesn't already exist
      if (!new_name %in% colnames(data)) {
        rename_list[[old_name]] <- new_name
        cat("    Will rename:", old_name, "->", new_name, "\n")
      } else {
        cat("    Skipping:", old_name, "-> target", new_name, "already exists\n")
      }
    }
  }
  
  # Apply renames
  if (length(rename_list) > 0) {
    for (old_name in names(rename_list)) {
      colnames(data)[colnames(data) == old_name] <- rename_list[[old_name]]
    }
  }
  
  return(data)
}

# CORRECTED variable mappings based on documentation
daily_station_variables <- list(
  # Core identifiers - keep both for now
  "fecha" = "date",
  "indicativo" = "station_id", 
  "idema" = "station_id",
  "nombre" = "station_name",
  "provincia" = "province",
  "altitud" = "altitude",
  
  # Temperature variables - correct names
  "tmed" = "temp_mean",
  "tmin" = "temp_min", 
  "tmax" = "temp_max",
  "horatmin" = "time_temp_min",
  "horatmax" = "time_temp_max",
  
  # Precipitation
  "prec" = "precipitation",
  
  # Wind variables - correct names from docs
  "dir" = "wind_direction",
  "velmedia" = "wind_speed",  # NOT wind_speed_mean
  "racha" = "wind_gust",      # NOT wind_gust_max
  "horaracha" = "time_wind_gust",  # NOT time_wind_gust_max
  
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
  
  # Solar radiation - correct name from docs
  "sol" = "solar_hours",  # NOT solar_radiation
  
  # Fix incorrect previous renames
  "wind_speed_mean" = "wind_speed",
  "wind_gust_max" = "wind_gust", 
  "time_wind_gust_max" = "time_wind_gust",
  "solar_radiation" = "solar_hours",
  
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

municipal_variables <- list(
  # Core identifiers - correct names
  "municipio_id" = "municipality_id",
  "municipio_nombre" = "municipality_name", 
  "municipio" = "municipality_id",
  "municipio_code" = "municipality_id",
  "provincia" = "province",
  "fecha" = "date",
  "elaborado" = "forecast_issued_at",
  
  # Temperature variables
  "temp_avg" = "temp_mean",
  "tmed_municipal" = "temp_mean",
  "temp_max" = "temp_max",
  "tmax_municipal" = "temp_max",
  "temp_min" = "temp_min",
  "tmin_municipal" = "temp_min",
  
  # Humidity - correct names from docs
  "humid_max" = "humidity_max",   # NOT hrMax
  "humid_min" = "humidity_min",   # NOT hrMin  
  "hrMedia_municipal" = "humidity_mean",
  
  # Wind
  "wind_speed" = "wind_speed",
  "velmedia_municipal" = "wind_speed",
  
  # Data source and priority
  "data_source" = "data_source",
  "source" = "data_source", 
  "priority" = "data_priority",
  
  # Quality control
  "temp_range_ok" = "qc_temp_range",
  "temp_realistic" = "qc_temp_realistic",
  
  # Metadata
  "collected_at" = "collection_timestamp",
  "processed_at" = "processing_timestamp",
  "n_stations" = "n_stations"
)

hourly_variables <- list(
  # Core identifiers
  "idema" = "station_id",
  "fint" = "datetime", 
  "date" = "date",
  "measure" = "variable_type",
  "value" = "value"
)

# Process each dataset with correct mapping
datasets <- list(
  list(file = "daily_station_historical.csv", vars = daily_station_variables),
  list(file = "daily_municipal_extended.csv", vars = municipal_variables),
  list(file = "hourly_station_ongoing.csv", vars = hourly_variables)
)

data_dir <- "/home/j.palmer/research/weather-data-collector-spain/data/output"

for (dataset in datasets) {
  dataset_file <- dataset$file
  variable_map <- dataset$vars
  file_path <- file.path(data_dir, dataset_file)
  
  if (!file.exists(file_path)) {
    cat("Warning: File", dataset_file, "not found\n")
    next
  }
  
  cat("\n=== Processing:", dataset_file, "===\n")
  
  tryCatch({
    # Read the data
    data <- read.csv(file_path, stringsAsFactors = FALSE)
    
    cat("  Original dimensions:", nrow(data), "rows,", ncol(data), "columns\n")
    cat("  Original columns:", paste(head(colnames(data), 10), collapse = ", "), "...\n")
    
    # Create backup
    backup_file <- paste0(file_path, ".backup_corrected_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy(file_path, backup_file)
    cat("  Backup created:", basename(backup_file), "\n")
    
    # Apply correct standardization
    data_corrected <- safe_rename_columns(data, variable_map)
    
    cat("  Final dimensions:", nrow(data_corrected), "rows,", ncol(data_corrected), "columns\n")
    cat("  Final columns:", paste(head(colnames(data_corrected), 10), collapse = ", "), "...\n")
    
    # Write corrected version
    write.csv(data_corrected, file_path, row.names = FALSE)
    cat("  ✅ Successfully corrected", dataset_file, "\n")
    
  }, error = function(e) {
    cat("  ❌ Error processing", dataset_file, ":", e$message, "\n")
  })
}

cat("\n🎯 Variable standardization corrected according to documentation!\n")
