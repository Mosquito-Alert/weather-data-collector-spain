#!/usr/bin/env Rscript

# aggregate_daily_stations_current.R
# Produces daily_stations_current.csv.gz from hourly data aggregated by day
# Covers the gap between historical data and present (typically T-4 days to yesterday)
# Keeps original AEMET variable names

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
})

cat("=== Daily Stations Current Aggregation ===\n")
cat("Source: Hourly station data aggregated to daily\n")
cat("Period: Gap between historical data end and yesterday\n")
cat("Variables: Original AEMET names preserved\n\n")

# Load hourly data
hourly_files <- c(
  "data/output/hourly_station_ongoing.csv.gz",
  "data/output/hourly_station_ongoing.csv"
)
hourly_file <- hourly_files[file.exists(hourly_files)][1]

if (is.na(hourly_file)) {
  cat("No hourly data found. Running hourly collection...\n")
  tryCatch({
    source("scripts/r/get_latest_data.R")
  }, error = function(e) {
    stop("Failed to collect hourly data: ", e$message)
  })
  hourly_file <- hourly_files[file.exists(hourly_files)][1]
  if (is.na(hourly_file)) {
    stop("Hourly data collection did not produce expected output")
  }
}

cat("Loading hourly data from:", basename(hourly_file), "\n")
hourly_data <- fread(hourly_file)

# Check hourly data structure
cat("Hourly data shape:", nrow(hourly_data), "rows,", ncol(hourly_data), "columns\n")
cat("Columns:", paste(names(hourly_data), collapse = ", "), "\n")

# Determine date column and datetime column
datetime_col <- NULL
date_col <- NULL
station_col <- NULL

if ("fint" %in% names(hourly_data)) datetime_col <- "fint"
if ("datetime" %in% names(hourly_data)) datetime_col <- "datetime"
if ("fecha" %in% names(hourly_data)) date_col <- "fecha"
if ("date" %in% names(hourly_data)) date_col <- "date"
if ("idema" %in% names(hourly_data)) station_col <- "idema"
if ("indicativo" %in% names(hourly_data)) station_col <- "indicativo"

if (is.null(datetime_col)) stop("No datetime column found in hourly data")
if (is.null(station_col)) stop("No station ID column found in hourly data")

# Convert datetime and create date if needed
hourly_data[[datetime_col]] <- as_datetime(hourly_data[[datetime_col]])
if (is.null(date_col)) {
  hourly_data$fecha <- as.Date(hourly_data[[datetime_col]])
  date_col <- "fecha"
}

cat("Using datetime column:", datetime_col, "\n")
cat("Using date column:", date_col, "\n")
cat("Using station column:", station_col, "\n")

# Check if data is in long format (measure/value) or wide format
if (all(c("measure", "value") %in% names(hourly_data))) {
  cat("Data is in long format, aggregating by measure...\n")
  
  # Aggregate hourly to daily by measure
  daily_aggregated <- hourly_data %>%
    group_by(!!sym(date_col), !!sym(station_col), measure) %>%
    summarise(
      value = case_when(
        measure %in% c("ta", "hr", "vv", "pres", "vd") ~ mean(value, na.rm = TRUE),  # Mean for these
        measure %in% c("tamax", "tmax") ~ max(value, na.rm = TRUE),                  # Maximum
        measure %in% c("tamin", "tmin") ~ min(value, na.rm = TRUE),                  # Minimum  
        measure %in% c("prec", "pcp") ~ sum(value, na.rm = TRUE),                    # Sum for precipitation
        TRUE ~ mean(value, na.rm = TRUE)                                             # Default to mean
      ),
      n_observations = n(),
      first_observation = min(!!sym(datetime_col), na.rm = TRUE),
      last_observation = max(!!sym(datetime_col), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(!is.na(value) & !is.infinite(value)) %>%
    as.data.table()
  
  # Reshape to wide format to match historical data structure
  wide_data <- dcast(daily_aggregated, 
                     formula = as.formula(paste(date_col, "+", station_col, "~ measure")), 
                     value.var = "value",
                     fun.aggregate = mean)
  
  # Add metadata columns
  metadata_cols <- daily_aggregated[, .(
    n_total_observations = sum(n_observations),
    first_observation = min(first_observation),
    last_observation = max(last_observation)
  ), by = c(date_col, station_col)]
  
  daily_current <- merge(wide_data, metadata_cols, by = c(date_col, station_col))
  
} else {
  cat("Data appears to be in wide format, aggregating directly...\n")
  
  # Group by date and station, aggregate numeric columns
  daily_current <- hourly_data %>%
    group_by(!!sym(date_col), !!sym(station_col)) %>%
    summarise(
      across(where(is.numeric), ~ if(length(unique(.)) == 1) first(.) else mean(., na.rm = TRUE)),
      n_observations = n(),
      first_observation = min(!!sym(datetime_col), na.rm = TRUE),
      last_observation = max(!!sym(datetime_col), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    as.data.table()
}

# Determine the gap period to cover
# Check if we have historical data to determine cutoff
hist_file <- "data/output/daily_stations_historical.csv.gz"
if (file.exists(hist_file)) {
  hist_sample <- fread(hist_file, nrows = 1000)
  if ("fecha" %in% names(hist_sample)) {
    hist_end_date <- max(as.Date(hist_sample$fecha), na.rm = TRUE)
    gap_start_date <- hist_end_date + days(1)
    cat("Historical data ends:", hist_end_date, "\n")
    cat("Gap period starts:", gap_start_date, "\n")
    
    # Filter to gap period only
    daily_current <- daily_current[get(date_col) >= gap_start_date]
  }
}

# Filter to complete days only (exclude today if partial)
yesterday <- Sys.Date() - days(1)
daily_current <- daily_current[get(date_col) <= yesterday]

cat("Daily current data shape:", nrow(daily_current), "rows,", ncol(daily_current), "columns\n")
if (nrow(daily_current) > 0) {
  cat("Date range:", min(daily_current[[date_col]], na.rm = TRUE), "to", max(daily_current[[date_col]], na.rm = TRUE), "\n")
  cat("Stations:", length(unique(daily_current[[station_col]])), "\n")
}

# Add metadata
daily_current$collection_timestamp <- Sys.time()
daily_current$data_source <- "hourly_aggregated"
daily_current$aggregation_method <- "mean_except_prec_sum_minmax_specific"

# Rename station column to match historical format
if (station_col != "indicativo") {
  setnames(daily_current, station_col, "indicativo")
}

# Save output
output_file <- "data/output/daily_stations_current.csv.gz"
fwrite(daily_current, output_file)

cat("\n=== Current Daily Aggregation Complete ===\n")
cat("Output file:", output_file, "\n")
cat("Records:", nrow(daily_current), "\n")
cat("File size:", round(file.size(output_file)/1024/1024, 1), "MB\n")
if (nrow(daily_current) > 0) {
  cat("Date range:", min(daily_current$fecha, na.rm = TRUE), "to", max(daily_current$fecha, na.rm = TRUE), "\n")
  cat("Stations:", length(unique(daily_current$indicativo)), "\n")
}
