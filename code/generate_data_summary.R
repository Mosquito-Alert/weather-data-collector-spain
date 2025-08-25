#!/usr/bin/env Rscript

# generate_data_summary.R
# Creates comprehensive data summary for README.md
# Updates automatically with each collection run

library(tidyverse)
library(lubridate)
library(data.table)

cat("=== GENERATING DATA SUMMARY ===\n")

# Helper function to format numbers with commas
format_number <- function(x) {
  format(x, big.mark = ",", scientific = FALSE)
}

# Helper function to calculate file sizes
get_file_size_mb <- function(filepath) {
  if(file.exists(filepath)) {
    round(file.size(filepath) / 1024 / 1024, 1)
  } else {
    0
  }
}

# === COLLECT DATA STATISTICS ===

summary_data = list(
  generation_time = format(Sys.time()),
  generation_date = as.character(Sys.Date())
)

# 1. Station Daily Data
cat("Analyzing station daily data...\n")
station_files = list.files("data/output", pattern = "daily_station_aggregated_.*\\.csv", full.names = TRUE)

if(length(station_files) > 0) {
  latest_station_file = station_files[which.max(file.mtime(station_files))]
  station_data = fread(latest_station_file)
  station_data$date = as.Date(station_data$date)
  
  summary_data$station_daily = list(
    total_records = nrow(station_data),
    unique_stations = length(unique(station_data$idema)),
    date_range_start = as.character(min(station_data$date, na.rm = TRUE)),
    date_range_end = as.character(max(station_data$date, na.rm = TRUE)),
    latest_file = basename(latest_station_file),
    file_size_mb = get_file_size_mb(latest_station_file),
    last_updated = as.character(as.Date(file.mtime(latest_station_file)))
  )
} else {
  summary_data$station_daily = list(
    total_records = 0,
    unique_stations = 0,
    latest_file = "Not found"
  )
}

# 2. Municipal Data
cat("Analyzing municipal data...\n")
municipal_files = list.files("data/output", pattern = "municipal_aggregated_.*\\.csv", full.names = TRUE)

if(length(municipal_files) > 0) {
  latest_municipal_file = municipal_files[which.max(file.mtime(municipal_files))]
  municipal_data = fread(latest_municipal_file)
  municipal_data$fecha = as.Date(municipal_data$fecha)
  
  # Separate historical vs forecast
  forecast_data = municipal_data[source == "forecast"]
  historical_data = municipal_data[source == "station_aggregated"]
  
  summary_data$municipal_data = list(
    total_records = nrow(municipal_data),
    unique_municipalities = length(unique(municipal_data$municipio_code)),
    historical_records = nrow(historical_data),
    forecast_records = nrow(forecast_data),
    date_range_start = as.character(min(municipal_data$fecha, na.rm = TRUE)),
    date_range_end = as.character(max(municipal_data$fecha, na.rm = TRUE)),
    forecast_coverage_days = ifelse(nrow(forecast_data) > 0, 
                                   as.numeric(max(forecast_data$fecha) - min(forecast_data$fecha)) + 1, 0),
    latest_file = basename(latest_municipal_file),
    file_size_mb = get_file_size_mb(latest_municipal_file),
    last_updated = as.character(as.Date(file.mtime(latest_municipal_file)))
  )
} else {
  summary_data$municipal_data = list(
    total_records = 0,
    unique_municipalities = 0,
    latest_file = "Not found"
  )
}

# 3. Hourly Data
cat("Analyzing hourly data...\n")
hourly_file = "data/output/hourly_station_ongoing.csv.gz"

if(file.exists(hourly_file)) {
  # Sample the hourly data for efficiency (it's large)
  hourly_sample = fread(hourly_file, nrows = 10000)
  
  # Get basic info without loading full file
  hourly_info = system2("gunzip", args = c("-c", hourly_file, "|", "wc", "-l"), stdout = TRUE)
  total_rows = as.numeric(hourly_info) - 1  # Subtract header
  
  # Get date range from sample
  hourly_sample$fint = as_datetime(hourly_sample$fint)
  unique_stations_sample = length(unique(hourly_sample$idema))
  
  summary_data$hourly_data = list(
    total_records = total_rows,
    unique_stations_sample = unique_stations_sample,
    unique_measures = length(unique(hourly_sample$measure)),
    date_range_start = as.character(as.Date(min(hourly_sample$fint, na.rm = TRUE))),
    date_range_end = as.character(as.Date(max(hourly_sample$fint, na.rm = TRUE))),
    file_size_mb = get_file_size_mb(hourly_file),
    last_updated = as.character(as.Date(file.mtime(hourly_file)))
  )
} else {
  summary_data$hourly_data = list(
    total_records = 0,
    unique_stations_sample = 0,
    file_size_mb = 0
  )
}

# 4. Recent Collection Performance
cat("Analyzing recent performance...\n")
gap_files = list.files("data/output", pattern = "gap_analysis_summary_.*\\.json", full.names = TRUE)

if(length(gap_files) > 0) {
  latest_gap_file = gap_files[which.max(file.mtime(gap_files))]
  gap_analysis = jsonlite::read_json(latest_gap_file)
  
  summary_data$data_quality = list(
    station_coverage_percent = gap_analysis$station_daily$coverage_percent,
    forecast_coverage_percent = gap_analysis$municipal_forecasts$coverage_percent,
    recent_hourly_observations = gap_analysis$hourly_continuity$recent_observations,
    last_gap_analysis = gap_analysis$analysis_date
  )
} else {
  summary_data$data_quality = list(
    station_coverage_percent = "Unknown",
    forecast_coverage_percent = "Unknown",
    last_gap_analysis = "Not available"
  )
}

# === GENERATE MARKDOWN SUMMARY ===
cat("Generating markdown summary...\n")

markdown_summary = paste0("
## 📊 Current Data Collection Status

*Last updated: ", summary_data$generation_time, "*

### Dataset 1: Daily Station Data
- **Records**: ", format_number(summary_data$station_daily$total_records), " station-days
- **Stations**: ", format_number(summary_data$station_daily$unique_stations), " weather stations
- **Coverage**: ", summary_data$station_daily$date_range_start, " to ", summary_data$station_daily$date_range_end, "
- **Data Quality**: ", ifelse(is.numeric(summary_data$data_quality$station_coverage_percent), 
                              paste0(summary_data$data_quality$station_coverage_percent, "% coverage"), 
                              "Coverage analysis pending"), "
- **Latest File**: `", summary_data$station_daily$latest_file, "` (", summary_data$station_daily$file_size_mb, " MB)

### Dataset 2: Municipal Daily Data  
- **Records**: ", format_number(summary_data$municipal_data$total_records), " municipality-days
- **Municipalities**: ", format_number(summary_data$municipal_data$unique_municipalities), " municipalities
- **Historical Data**: ", format_number(summary_data$municipal_data$historical_records), " records
- **Forecast Data**: ", format_number(summary_data$municipal_data$forecast_records), " records (", 
summary_data$municipal_data$forecast_coverage_days, " days coverage)
- **Coverage**: ", summary_data$municipal_data$date_range_start, " to ", summary_data$municipal_data$date_range_end, "
- **Data Quality**: ", ifelse(is.numeric(summary_data$data_quality$forecast_coverage_percent),
                              paste0(summary_data$data_quality$forecast_coverage_percent, "% forecast coverage"),
                              "Coverage analysis pending"), "
- **Latest File**: `", summary_data$municipal_data$latest_file, "` (", summary_data$municipal_data$file_size_mb, " MB)

### Dataset 3: Hourly Station Data
- **Records**: ", format_number(summary_data$hourly_data$total_records), " hourly observations
- **Stations**: ~", format_number(summary_data$hourly_data$unique_stations_sample), " stations (sample estimate)
- **Variables**: ", summary_data$hourly_data$unique_measures, " meteorological measures
- **Coverage**: ", summary_data$hourly_data$date_range_start, " to ", summary_data$hourly_data$date_range_end, "
- **Recent Activity**: ", ifelse(is.numeric(summary_data$data_quality$recent_hourly_observations),
                                format_number(summary_data$data_quality$recent_hourly_observations),
                                "Analysis pending"), " observations (last 30 days)
- **Archive Size**: ", summary_data$hourly_data$file_size_mb, " MB compressed

### 🔄 Collection System Status
- **Collection Method**: Hybrid system using `climaemet` package + custom API calls
- **Performance**: ~5.4x faster than previous approach
- **Schedule**: Daily collection at 2 AM via crontab
- **Last Gap Analysis**: ", ifelse(!is.null(summary_data$data_quality$last_gap_analysis),
                                  summary_data$data_quality$last_gap_analysis,
                                  "Pending"), "

### 📈 Data Growth Tracking
| Dataset | Current Size | Growth Rate | Last Updated |
|---------|-------------|-------------|--------------|
| Station Daily | ", summary_data$station_daily$file_size_mb, " MB | ~", 
round(summary_data$station_daily$total_records / max(1, as.numeric(Sys.Date() - as.Date(summary_data$station_daily$date_range_start))), 0), 
" records/day | ", summary_data$station_daily$last_updated, " |
| Municipal Data | ", summary_data$municipal_data$file_size_mb, " MB | ~",
round(summary_data$municipal_data$total_records / max(1, as.numeric(Sys.Date() - as.Date(summary_data$municipal_data$date_range_start))), 0),
" records/day | ", summary_data$municipal_data$last_updated, " |
| Hourly Archive | ", summary_data$hourly_data$file_size_mb, " MB | ~",
ifelse(is.numeric(summary_data$data_quality$recent_hourly_observations),
       round(summary_data$data_quality$recent_hourly_observations / 30, 0),
       "TBD"), " records/day | ", summary_data$hourly_data$last_updated, " |

---
")

# Save summary data as JSON for programmatic access
summary_file = paste0("data/output/data_summary_", Sys.Date(), ".json")
jsonlite::write_json(summary_data, summary_file, pretty = TRUE, auto_unbox = TRUE)
cat("Summary data saved to:", summary_file, "\n")

# Save markdown fragment
markdown_file = "data/output/current_data_summary.md"
writeLines(markdown_summary, markdown_file)
cat("Markdown summary saved to:", markdown_file, "\n")

cat("✅ Data summary generation complete.\n")
cat("\nTo update README.md, insert the contents of", markdown_file, "\n")
cat("at the desired location in your main README.md file.\n")
