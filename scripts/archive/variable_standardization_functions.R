#!/usr/bin/env Rscript

# Variable standardization functions for use during data aggregation
# This ensures consistent naming before data gets combined

# Standard variable mapping based on documentation
get_standard_station_mapping <- function() {
  list(
    # Core identifiers
    "indicativo" = "station_id",
    "idema" = "station_id", 
    "nombre" = "station_name",
    "provincia" = "province",
    "altitud" = "altitude",
    "fecha" = "date",
    
    # Temperature variables
    "tmed" = "temp_mean",
    "tmin" = "temp_min", 
    "tmax" = "temp_max",
    "ta" = "temp_mean",  # hourly temp
    "tamin" = "temp_min",
    "tamax" = "temp_max",
    "horatmin" = "time_temp_min",
    "horatmax" = "time_temp_max",
    
    # Precipitation
    "prec" = "precipitation",
    
    # Wind variables
    "dir" = "wind_direction",
    "velmedia" = "wind_speed",
    "vv" = "wind_speed",  # hourly wind
    "racha" = "wind_gust",
    "horaracha" = "time_wind_gust",
    
    # Atmospheric pressure
    "presMax" = "pressure_max",
    "horaPresMax" = "time_pressure_max", 
    "presMin" = "pressure_min",
    "horaPresMin" = "time_pressure_min",
    "pres" = "pressure",  # hourly pressure
    
    # Humidity
    "hrMedia" = "humidity_mean",
    "hrMax" = "humidity_max",
    "hrMin" = "humidity_min", 
    "hr" = "humidity_mean",  # hourly humidity
    "horaHrMax" = "time_humidity_max",
    "horaHrMin" = "time_humidity_min",
    
    # Solar radiation
    "sol" = "solar_hours"
  )
}

get_standard_municipal_mapping <- function() {
  list(
    # Core identifiers
    "municipio_id" = "municipality_id",
    "municipio_nombre" = "municipality_name",
    "municipio" = "municipality_id",
    "municipio_code" = "municipality_id",
    "provincia" = "province",
    "fecha" = "date",
    "elaborado" = "forecast_issued_at",
    
    # Temperature variables - forecast names
    "temperatura_maxima" = "temp_max",
    "temperatura_minima" = "temp_min", 
    "temperatura_dato" = "temp_mean",
    "temp_max" = "temp_max",
    "temp_min" = "temp_min",
    "temp_avg" = "temp_mean",
    
    # Humidity variables - forecast names  
    "humedad_maxima" = "humidity_max",
    "humedad_minima" = "humidity_min",
    "humedad_dato" = "humidity_mean",
    "humid_max" = "humidity_max",
    "humid_min" = "humidity_min",
    
    # Wind
    "racha_max" = "wind_speed",
    "wind_speed" = "wind_speed",
    
    # Precipitation
    "prob_precipitacion" = "precipitation_prob"
  )
}

# Function to apply standardization during data processing
standardize_columns <- function(data, mapping) {
  current_cols <- colnames(data)
  
  # Apply renames only for columns that exist
  for (old_name in current_cols) {
    if (old_name %in% names(mapping)) {
      new_name <- mapping[[old_name]]
      # Only rename if new name doesn't already exist
      if (!new_name %in% colnames(data)) {
        colnames(data)[colnames(data) == old_name] <- new_name
      }
    }
  }
  
  return(data)
}

# Function to select and rename key columns for joining
prepare_station_data_for_joining <- function(station_data) {
  mapping <- get_standard_station_mapping()
  
  # Standardize column names first
  station_data <- standardize_columns(station_data, mapping)
  
  # Select core columns with standard names
  core_cols <- c("station_id", "station_name", "province", "altitude", "date", 
                 "temp_mean", "temp_max", "temp_min", "precipitation",
                 "humidity_mean", "humidity_max", "humidity_min",
                 "wind_speed", "wind_direction", "wind_gust",
                 "pressure_max", "pressure_min", "solar_hours")
  
  # Select only columns that exist
  available_cols <- intersect(core_cols, colnames(station_data))
  
  return(station_data[, available_cols, drop = FALSE])
}

prepare_municipal_data_for_joining <- function(municipal_data) {
  mapping <- get_standard_municipal_mapping()
  
  # Standardize column names first
  municipal_data <- standardize_columns(municipal_data, mapping)
  
  # Select core columns with standard names
  core_cols <- c("municipality_id", "municipality_name", "province", "date",
                 "temp_mean", "temp_max", "temp_min", 
                 "humidity_mean", "humidity_max", "humidity_min",
                 "wind_speed", "precipitation_prob", "forecast_issued_at")
  
  # Select only columns that exist
  available_cols <- intersect(core_cols, colnames(municipal_data))
  
  return(municipal_data[, available_cols, drop = FALSE])
}

cat("Variable standardization functions loaded.\n")
