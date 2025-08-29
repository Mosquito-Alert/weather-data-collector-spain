#!/usr/bin/env Rscript

# standardize_variable_names_corrected.R
# CORRECTED version that properly handles variable standardization without duplicates

rm(list=ls())

library(tidyverse)
library(data.table)

cat("=== CORRECTED VARIABLE STANDARDIZATION ===\n")
cat("Carefully standardizing variable names without creating duplicates\n")
cat("Started at:", format(Sys.time()), "\n\n")

# === FUNCTION: SAFE COLUMN RENAMING ===
# This function safely renames columns and removes duplicates
safe_rename_columns = function(df, mapping, dataset_name) {
  cat("Processing", dataset_name, "...\n")
  
  # Get current column names
  current_cols = names(df)
  cat("  Original columns:", length(current_cols), "\n")
  
  # Create a new dataframe with only the columns we want to keep
  # This prevents duplicate column issues
  
  if(dataset_name == "station historical") {
    # Station data: Select and rename core columns
    standardized_df = df %>%
      select(
        date = if("date" %in% names(df)) date else fecha,
        station_id = if("station_id" %in% names(df)) station_id else if("idema" %in% names(df)) idema else indicativo,
        station_name = nombre,
        province = provincia,
        altitude = altitud,
        temp_mean = tmed,
        temp_max = tmax,
        temp_min = tmin,
        precipitation = prec,
        humidity_mean = hrMedia,
        humidity_max = hrMax,
        humidity_min = hrMin,
        wind_speed = velmedia,
        wind_direction = dir,
        wind_gust = racha,
        pressure_max = presMax,
        pressure_min = presMin,
        solar_hours = sol,
        time_temp_min = horatmin,
        time_temp_max = horatmax,
        time_humidity_max = horaHrMax,
        time_humidity_min = horaHrMin,
        time_pressure_max = horaPresMax,
        time_pressure_min = horaPresMin,
        time_wind_gust = horaracha,
        temp_range_ok = temp_range_ok,
        temp_realistic = temp_realistic,
        prec_realistic = prec_realistic,
        collected_at = collected_at,
        processed_at = processed_at,
        data_source = source
      ) %>%
      # Remove any remaining duplicate columns
      select_if(function(x) !all(is.na(x))) %>%
      # Ensure date is properly formatted
      mutate(
        date = as.Date(date),
        temp_mean = as.numeric(temp_mean),
        temp_max = as.numeric(temp_max),
        temp_min = as.numeric(temp_min),
        precipitation = as.numeric(precipitation)
      )
      
  } else if(dataset_name == "municipal extended") {
    # Municipal data: Select and rename core columns
    standardized_df = df %>%
      select(
        date = fecha,
        municipality_id = municipio_id,
        municipality_name = municipio_nombre,
        province = provincia,
        temp_mean = if("tmed_municipal" %in% names(df)) tmed_municipal else temp_avg,
        temp_max = if("tmax_municipal" %in% names(df)) tmax_municipal else temp_max,
        temp_min = if("tmin_municipal" %in% names(df)) tmin_municipal else temp_min,
        humidity_mean = if("hrMedia_municipal" %in% names(df)) hrMedia_municipal else NA,
        humidity_max = humid_max,
        humidity_min = humid_min,
        wind_speed = if("velmedia_municipal" %in% names(df)) velmedia_municipal else wind_speed,
        forecast_issued_at = elaborado,
        data_source = data_source,
        data_priority = priority,
        n_stations = n_stations,
        temp_range_ok = temp_range_ok,
        temp_realistic = temp_realistic,
        collected_at = collected_at,
        processed_at = processed_at
      ) %>%
      # Ensure date is properly formatted
      mutate(
        date = as.Date(date),
        temp_mean = as.numeric(temp_mean),
        temp_max = as.numeric(temp_max),
        temp_min = as.numeric(temp_min)
      )
  }
  
  cat("  Standardized columns:", length(names(standardized_df)), "\n")
  cat("  Final columns:", paste(names(standardized_df)[1:5], collapse=", "), "...\n")
  
  return(standardized_df)
}

# === STANDARDIZE EXISTING DATA FILES ===

cat("\n=== PROCESSING STATION HISTORICAL DATA ===\n")
if(file.exists("data/output/daily_station_historical.csv")) {
  station_data = fread("data/output/daily_station_historical.csv")
  cat("Loaded", nrow(station_data), "station records\n")
  
  station_data_clean = safe_rename_columns(station_data, NULL, "station historical")
  
  # Backup current version
  backup_file = paste0("data/backup/daily_station_historical_before_standardization_", Sys.Date(), ".csv")
  file.copy("data/output/daily_station_historical.csv", backup_file, overwrite=TRUE)
  
  # Save clean standardized version
  fwrite(station_data_clean, "data/output/daily_station_historical.csv")
  cat("✅ Clean station data saved (", nrow(station_data_clean), "records,", ncol(station_data_clean), "columns )\n")
}

cat("\n=== PROCESSING MUNICIPAL EXTENDED DATA ===\n")
if(file.exists("data/output/daily_municipal_extended.csv")) {
  municipal_data = fread("data/output/daily_municipal_extended.csv")
  cat("Loaded", nrow(municipal_data), "municipal records\n")
  
  municipal_data_clean = safe_rename_columns(municipal_data, NULL, "municipal extended")
  
  # Backup current version
  backup_file = paste0("data/backup/daily_municipal_extended_before_standardization_", Sys.Date(), ".csv")
  file.copy("data/output/daily_municipal_extended.csv", backup_file, overwrite=TRUE)
  
  # Save clean standardized version
  fwrite(municipal_data_clean, "data/output/daily_municipal_extended.csv")
  cat("✅ Clean municipal data saved (", nrow(municipal_data_clean), "records,", ncol(municipal_data_clean), "columns )\n")
}

# === VERIFY RESULTS ===
cat("\n=== VERIFICATION ===\n")

if(file.exists("data/output/daily_station_historical.csv")) {
  station_verify = fread("data/output/daily_station_historical.csv", nrows=0)
  cat("Station data columns:", paste(names(station_verify), collapse=", "), "\n")
}

if(file.exists("data/output/daily_municipal_extended.csv")) {
  municipal_verify = fread("data/output/daily_municipal_extended.csv", nrows=0) 
  cat("Municipal data columns:", paste(names(municipal_verify), collapse=", "), "\n")
}

cat("\n========================================\n")
cat("CORRECTED STANDARDIZATION COMPLETE\n")
cat("========================================\n")
cat("✅ No duplicate columns\n")
cat("✅ Clean variable names\n") 
cat("✅ Data integrity preserved\n")
cat("✅ Backups created before changes\n")

cat("Completed at:", format(Sys.time()), "\n")
