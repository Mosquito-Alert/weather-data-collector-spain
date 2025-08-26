#!/usr/bin/env Rscript

# Robust standardization script for weather data
# Handles various date/time formats and column duplicates

library(dplyr, warn.conflicts = FALSE)
library(data.table, warn.conflicts = FALSE)

# Function to safely rename columns while avoiding date parsing errors
safe_rename_columns <- function(data, variable_map) {
  
  # Get current column names
  current_cols <- colnames(data)
  
  # Create a mapping for renaming
  rename_list <- list()
  
  for (old_name in current_cols) {
    if (old_name %in% names(variable_map)) {
      new_name <- variable_map[[old_name]]
      
      # If new name already exists, skip to avoid duplicates
      if (!new_name %in% names(rename_list)) {
        rename_list[[old_name]] <- new_name
      } else {
        cat("Warning: Skipping rename of", old_name, "to", new_name, "- target already exists\n")
      }
    }
  }
  
  # Apply renames if any
  if (length(rename_list) > 0) {
    data <- data %>% rename(!!!rename_list)
  }
  
  return(data)
}

# Function to remove duplicate columns (keeping first occurrence)
remove_duplicate_columns <- function(data) {
  col_names <- colnames(data)
  
  # Find duplicated column names
  duplicated_cols <- duplicated(col_names)
  
  if (any(duplicated_cols)) {
    cat("Removing", sum(duplicated_cols), "duplicate columns\n")
    data <- data[, !duplicated_cols, drop = FALSE]
  }
  
  return(data)
}

# Standardized variable mapping
standard_variables <- list(
  # Core identifiers
  "fecha" = "date",
  "indicativo" = "station_id", 
  "idema" = "station_id",
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
  
  # Read the data
  tryCatch({
    data <- fread(file_path, stringsAsFactors = FALSE)
    
    cat("  Original dimensions:", nrow(data), "rows,", ncol(data), "columns\n")
    cat("  Original columns:", paste(head(colnames(data), 10), collapse = ", "), "...\n")
    
    # Remove duplicate columns first
    data <- remove_duplicate_columns(data)
    
    # Apply standardized variable names
    data_standardized <- safe_rename_columns(data, standard_variables)
    
    cat("  Standardized dimensions:", nrow(data_standardized), "rows,", ncol(data_standardized), "columns\n")
    cat("  Standardized columns:", paste(head(colnames(data_standardized), 10), collapse = ", "), "...\n")
    
    # Create backup of original
    backup_file <- paste0(file_path, ".backup_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy(file_path, backup_file)
    cat("  Backup created:", basename(backup_file), "\n")
    
    # Write standardized version
    fwrite(data_standardized, file_path)
    cat("  ✓ Successfully standardized", dataset_file, "\n\n")
    
  }, error = function(e) {
    cat("  ✗ Error processing", dataset_file, ":", e$message, "\n\n")
  })
}

cat("Variable standardization completed!\n")
