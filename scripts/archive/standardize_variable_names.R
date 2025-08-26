#!/usr/bin/env Rscript

# standardize_variable_names.R
# Standardizes variable names across all three weather datasets
# Creates both standardized versions and updates collection scripts

rm(list=ls())

library(tidyverse)
library(data.table)

cat("=== WEATHER DATA VARIABLE STANDARDIZATION ===\n")
cat("Standardizing variable names across all three datasets\n")
cat("Started at:", format(Sys.time()), "\n\n")

# === VARIABLE MAPPING DEFINITIONS ===

# Station historical data mapping
station_mapping = list(
  # Identifiers
  "indicativo" = "station_id",
  "idema" = "station_id", 
  "fecha" = "date",
  
  # Core weather variables
  "tmed" = "temp_mean",
  "tmax" = "temp_max", 
  "tmin" = "temp_min",
  "prec" = "precipitation",
  "hrMedia" = "humidity_mean",
  "hrMax" = "humidity_max",
  "hrMin" = "humidity_min",
  "velmedia" = "wind_speed",
  "presMax" = "pressure_max",
  "presMin" = "pressure_min",
  
  # Location info (keep as-is but standardized)
  "nombre" = "station_name",
  "provincia" = "province",
  "altitud" = "altitude",
  
  # Time variables (keep for precision)
  "horatmin" = "time_temp_min",
  "horatmax" = "time_temp_max", 
  "horaHrMax" = "time_humidity_max",
  "horaHrMin" = "time_humidity_min",
  "horaPresMax" = "time_pressure_max",
  "horaPresMin" = "time_pressure_min",
  "horaracha" = "time_wind_gust",
  
  # Wind variables
  "dir" = "wind_direction",
  "racha" = "wind_gust",
  
  # Solar radiation
  "sol" = "solar_hours",
  
  # Quality control (keep as-is)
  "temp_range_ok" = "temp_range_ok",
  "temp_realistic" = "temp_realistic", 
  "prec_realistic" = "prec_realistic",
  
  # Metadata (keep as-is)
  "collected_at" = "collected_at",
  "processed_at" = "processed_at",
  "source" = "data_source",
  "measure" = "measure",
  "value" = "value",
  "n_observations" = "n_observations"
)

# Municipal extended data mapping  
municipal_mapping = list(
  # Identifiers (IMPORTANT: Document municipality code source)
  "municipio_id" = "municipality_id",  # CUMUN code from AEMET
  "municipio" = "municipality_id",     # Alternative name
  "municipio_code" = "municipality_id", # Standardized name
  "municipio_nombre" = "municipality_name",
  "fecha" = "date",
  "provincia" = "province",
  
  # Temperature variables
  "temp_max" = "temp_max",
  "temp_min" = "temp_min", 
  "temp_avg" = "temp_mean",
  "tmed_municipal" = "temp_mean",
  "tmax_municipal" = "temp_max",
  "tmin_municipal" = "temp_min",
  
  # Humidity variables  
  "humid_max" = "humidity_max",
  "humid_min" = "humidity_min",
  "hrMedia_municipal" = "humidity_mean",
  
  # Wind variables
  "wind_speed" = "wind_speed",
  "velmedia_municipal" = "wind_speed",
  
  # Data source tracking
  "data_source" = "data_source",
  "source" = "data_source", 
  "elaborado" = "forecast_issued_at",
  "collected_at" = "collected_at",
  "processed_at" = "processed_at",
  
  # Quality control
  "temp_range_ok" = "temp_range_ok",
  "temp_realistic" = "temp_realistic",
  "n_stations" = "n_stations",
  "priority" = "data_priority"
)

# Hourly data mapping
hourly_mapping = list(
  "fint" = "datetime",
  "idema" = "station_id", 
  "date" = "date",
  "measure" = "variable_type",
  "value" = "value"
)

# === FUNCTION: STANDARDIZE COLUMNS ===
standardize_columns = function(df, mapping, dataset_name) {
  cat("Standardizing", dataset_name, "...\n")
  original_cols = names(df)
  
  # Apply mapping
  for(old_name in names(mapping)) {
    if(old_name %in% names(df)) {
      new_name = mapping[[old_name]]
      names(df)[names(df) == old_name] = new_name
      cat("  ", old_name, "->", new_name, "\n")
    }
  }
  
  # Report unmapped columns
  unmapped = setdiff(original_cols, names(mapping))
  if(length(unmapped) > 0) {
    cat("  Unmapped columns (kept as-is):", paste(unmapped, collapse=", "), "\n")
  }
  
  return(df)
}

# === STANDARDIZE EXISTING DATA FILES ===

cat("\n=== STANDARDIZING EXISTING DATA FILES ===\n")

# 1. Daily Station Historical
if(file.exists("data/output/daily_station_historical.csv")) {
  cat("\n1. Processing daily_station_historical.csv...\n")
  station_data = fread("data/output/daily_station_historical.csv")
  station_data_std = standardize_columns(station_data, station_mapping, "station historical")
  
  # Backup original
  file.copy("data/output/daily_station_historical.csv", 
            "data/backup/daily_station_historical_original.csv", overwrite=TRUE)
  
  # Save standardized version
  fwrite(station_data_std, "data/output/daily_station_historical.csv")
  cat("  ✅ Standardized version saved (", nrow(station_data_std), "records )\n")
}

# 2. Daily Municipal Extended  
if(file.exists("data/output/daily_municipal_extended.csv")) {
  cat("\n2. Processing daily_municipal_extended.csv...\n")
  municipal_data = fread("data/output/daily_municipal_extended.csv")
  municipal_data_std = standardize_columns(municipal_data, municipal_mapping, "municipal extended")
  
  # Backup original
  file.copy("data/output/daily_municipal_extended.csv",
            "data/backup/daily_municipal_extended_original.csv", overwrite=TRUE)
  
  # Save standardized version
  fwrite(municipal_data_std, "data/output/daily_municipal_extended.csv")
  cat("  ✅ Standardized version saved (", nrow(municipal_data_std), "records )\n")
}

# 3. Hourly Station Ongoing
if(file.exists("data/output/hourly_station_ongoing.csv")) {
  cat("\n3. Processing hourly_station_ongoing.csv...\n")
  hourly_data = fread("data/output/hourly_station_ongoing.csv")
  if(nrow(hourly_data) > 0) {
    hourly_data_std = standardize_columns(hourly_data, hourly_mapping, "hourly station")
    
    # Backup original
    file.copy("data/output/hourly_station_ongoing.csv",
              "data/backup/hourly_station_ongoing_original.csv", overwrite=TRUE)
    
    # Save standardized version
    fwrite(hourly_data_std, "data/output/hourly_station_ongoing.csv")
    cat("  ✅ Standardized version saved (", nrow(hourly_data_std), "records )\n")
  } else {
    cat("  ℹ️ File exists but is empty, skipping\n")
  }
}

# === CREATE DOCUMENTATION ===
cat("\n=== CREATING VARIABLE DOCUMENTATION ===\n")

doc_content = "# Variable Standardization Documentation

## Overview
This document describes the standardized variable names used across all three weather datasets and their mapping from original AEMET variable names.

## Municipality Code Information
**IMPORTANT**: The municipality_id field uses CUMUN codes from AEMET's municipal forecast system. 
- Source: AEMET OpenData API municipal forecasts
- Format: 5-digit numeric code
- Coverage: All Spanish municipalities (~8,000+)
- Note: This differs from INE codes - use appropriate conversion if merging with other Spanish administrative datasets

## Dataset 1: Daily Station Historical (`daily_station_historical.csv`)

### Weather Variables
| Standard Name | Original AEMET | Description | Units |
|---------------|----------------|-------------|-------|
| temp_mean | tmed | Daily mean temperature | °C |
| temp_max | tmax | Daily maximum temperature | °C |
| temp_min | tmin | Daily minimum temperature | °C |
| precipitation | prec | Daily precipitation | mm |
| humidity_mean | hrMedia | Daily mean relative humidity | % |
| humidity_max | hrMax | Daily maximum relative humidity | % |
| humidity_min | hrMin | Daily minimum relative humidity | % |
| wind_speed | velmedia | Daily mean wind speed | km/h |
| wind_direction | dir | Daily predominant wind direction | degrees |
| wind_gust | racha | Daily maximum wind gust | km/h |
| pressure_max | presMax | Daily maximum pressure | hPa |
| pressure_min | presMin | Daily minimum pressure | hPa |
| solar_hours | sol | Daily sunshine hours | hours |

### Timing Variables
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| time_temp_min | horatmin | Time of minimum temperature | HH:MM |
| time_temp_max | horatmax | Time of maximum temperature | HH:MM |
| time_humidity_max | horaHrMax | Time of maximum humidity | HH:MM |
| time_humidity_min | horaHrMin | Time of minimum humidity | HH:MM |
| time_pressure_max | horaPresMax | Time of maximum pressure | HH:MM |
| time_pressure_min | horaPresMin | Time of minimum pressure | HH:MM |
| time_wind_gust | horaracha | Time of maximum wind gust | HH:MM |

### Identifiers & Metadata
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| station_id | indicativo/idema | Unique station identifier |
| station_name | nombre | Station name |
| province | provincia | Province name |
| altitude | altitud | Station altitude (meters) |
| date | fecha | Date of observation | YYYY-MM-DD |

## Dataset 2: Daily Municipal Extended (`daily_municipal_extended.csv`)

### Weather Variables
| Standard Name | Original AEMET | Description | Units |
|---------------|----------------|-------------|-------|
| temp_mean | temp_avg/tmed_municipal | Daily mean temperature | °C |
| temp_max | temp_max/tmax_municipal | Daily maximum temperature | °C |
| temp_min | temp_min/tmin_municipal | Daily minimum temperature | °C |
| humidity_mean | hrMedia_municipal | Daily mean relative humidity | % |
| humidity_max | humid_max | Daily maximum relative humidity | % |
| humidity_min | humid_min | Daily minimum relative humidity | % |
| wind_speed | wind_speed/velmedia_municipal | Daily mean wind speed | km/h |

### Identifiers & Metadata
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| municipality_id | municipio_id/municipio/municipio_code | CUMUN municipality code |
| municipality_name | municipio_nombre | Municipality name |
| province | provincia | Province name |
| date | fecha | Date of observation/forecast | YYYY-MM-DD |
| forecast_issued_at | elaborado | When forecast was issued | ISO datetime |
| data_source | data_source/source | 'forecast' or 'station_aggregated' |
| data_priority | priority | Data priority (1=station, 2=forecast) |

## Dataset 3: Hourly Station Ongoing (`hourly_station_ongoing.csv`)

| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| station_id | idema | Unique station identifier |
| datetime | fint | Observation datetime | ISO datetime |
| date | date | Observation date | YYYY-MM-DD |
| variable_type | measure | Type of measurement |
| value | value | Measured value |

## Data Priority Logic

In the municipal dataset, when both forecast and station-aggregated data exist for the same municipality and date:
1. **Station-aggregated data takes precedence** (data_priority = 1)
2. **Forecast data is secondary** (data_priority = 2)

This ensures that actual measurements replace forecasts as they become available.

## Quality Control Variables

- **temp_range_ok**: Temperature range passes basic sanity checks
- **temp_realistic**: Temperature values are realistic for Spain
- **prec_realistic**: Precipitation values are realistic
- **n_stations**: Number of stations used for municipal aggregation

---
Generated on: $(date)
"

writeLines(doc_content, "docs/variable_standardization.md")
cat("✅ Documentation saved to docs/variable_standardization.md\n")

# === SUMMARY ===
cat("\n========================================\n")
cat("VARIABLE STANDARDIZATION COMPLETE\n") 
cat("========================================\n")
cat("Files processed:\n")
if(file.exists("data/output/daily_station_historical.csv")) {
  cat("  ✅ daily_station_historical.csv (standardized)\n")
}
if(file.exists("data/output/daily_municipal_extended.csv")) {
  cat("  ✅ daily_municipal_extended.csv (standardized)\n") 
}
if(file.exists("data/output/hourly_station_ongoing.csv")) {
  cat("  ✅ hourly_station_ongoing.csv (standardized)\n")
}

cat("\nOriginal files backed up to data/backup/\n")
cat("Documentation created: docs/variable_standardization.md\n")
cat("\nNext step: Update collection scripts to use standardized names\n")

cat("Completed at:", format(Sys.time()), "\n")
