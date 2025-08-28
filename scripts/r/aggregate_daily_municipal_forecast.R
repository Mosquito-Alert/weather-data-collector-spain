#!/usr/bin/env Rscript

# aggregate_daily_municipal_forecast.R
# Produces daily_municipal_forecast.csv.gz from AEMET municipal forecast API
# Keeps original variable names and accumulates forecast data over time for validation

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
})

cat("=== Daily Municipal Forecast Collection ===\n")
cat("Source: AEMET municipal forecast API\n")
cat("Purpose: Ongoing collection of forecast data for validation\n")
cat("Variables: Original AEMET names preserved\n\n")

# Check for existing forecast data to append to
existing_forecast_file <- "data/output/daily_municipal_forecast.csv.gz"
existing_data <- NULL

if (file.exists(existing_forecast_file)) {
  cat("Loading existing forecast data...\n")
  existing_data <- fread(existing_forecast_file)
  cat("Existing records:", nrow(existing_data), "\n")
  
  if (nrow(existing_data) > 0) {
    # Check date range of existing data
    date_col <- ifelse("fecha" %in% names(existing_data), "fecha", "date")
    if (date_col %in% names(existing_data)) {
      existing_data[[date_col]] <- as.Date(existing_data[[date_col]])
      cat("Existing data range:", min(existing_data[[date_col]], na.rm = TRUE), 
          "to", max(existing_data[[date_col]], na.rm = TRUE), "\n")
    }
  }
}

# Collect new forecast data
cat("Collecting new forecast data...\n")

# Try different forecast collection scripts
forecast_collected <- FALSE
new_forecast_data <- NULL

# Try climaemet-based collection first (fastest)
forecast_scripts <- c(
  "scripts/r/get_forecast_data.R",
  "scripts/archive/get_forecast_data_climaemet.R",
  "scripts/archive/get_forecast_data_hybrid.R"
)

for (script in forecast_scripts) {
  if (file.exists(script) && !forecast_collected) {
    cat("Trying forecast collection with:", basename(script), "\n")
    tryCatch({
      source(script)
      forecast_collected <- TRUE
      cat("✅ Forecast collection successful\n")
      break
    }, error = function(e) {
      cat("❌ Failed with", basename(script), ":", e$message, "\n")
    })
  }
}

if (!forecast_collected) {
  stop("All forecast collection methods failed")
}

# Look for newly collected forecast data
forecast_patterns <- c(
  "municipal.*forecast.*\\.csv(\\.gz)?$",
  "forecast.*municipal.*\\.csv(\\.gz)?$", 
  "municipal.*\\.csv(\\.gz)?$"
)

new_files <- NULL
for (pattern in forecast_patterns) {
  potential_files <- list.files("data/output", pattern = pattern, full.names = TRUE)
  if (length(potential_files) > 0) {
    # Get most recent file
    most_recent <- potential_files[which.max(file.mtime(potential_files))]
    
    # Check if it's newer than our target file (if it exists)
    if (!file.exists(existing_forecast_file) || 
        file.mtime(most_recent) > file.mtime(existing_forecast_file)) {
      new_files <- c(new_files, most_recent)
    }
  }
}

if (length(new_files) == 0) {
  stop("No new forecast data files found after collection")
}

# Load the most recent new forecast file
new_file <- new_files[which.max(file.mtime(new_files))]
cat("Loading new forecast data from:", basename(new_file), "\n")
new_forecast_data <- fread(new_file)

cat("New forecast data:", nrow(new_forecast_data), "records\n")
cat("Columns:", paste(names(new_forecast_data), collapse = ", "), "\n")

# Validate new data structure
date_col <- NULL
muni_col <- NULL
if ("fecha" %in% names(new_forecast_data)) date_col <- "fecha"
if ("date" %in% names(new_forecast_data)) date_col <- "date"
if ("municipio" %in% names(new_forecast_data)) muni_col <- "municipio"
if ("municipio_id" %in% names(new_forecast_data)) muni_col <- "municipio_id"
if ("municipality_id" %in% names(new_forecast_data)) muni_col <- "municipality_id"

if (is.null(date_col)) stop("No date column found in new forecast data")
if (is.null(muni_col)) stop("No municipality column found in new forecast data")

# Ensure date is Date type
new_forecast_data[[date_col]] <- as.Date(new_forecast_data[[date_col]])

# Add collection metadata
if (!"collection_timestamp" %in% names(new_forecast_data)) {
  new_forecast_data$collection_timestamp <- Sys.time()
}
if (!"data_source" %in% names(new_forecast_data)) {
  new_forecast_data$data_source <- "aemet_municipal_forecast_api"
}

# Combine with existing data if available
if (!is.null(existing_data) && nrow(existing_data) > 0) {
  cat("Combining with existing forecast data...\n")
  
  # Ensure column compatibility
  common_cols <- intersect(names(existing_data), names(new_forecast_data))
  
  if (length(common_cols) < 3) {
    cat("⚠️  Warning: Limited column overlap between existing and new data\n")
    cat("Common columns:", paste(common_cols, collapse = ", "), "\n")
  }
  
  # Use common columns for both datasets
  existing_subset <- existing_data[, ..common_cols]
  new_subset <- new_forecast_data[, ..common_cols]
  
  # Combine
  combined_forecast <- rbind(existing_subset, new_subset, fill = TRUE)
  
  # Remove duplicates based on key columns
  key_cols <- c(date_col, muni_col)
  if ("elaborado" %in% common_cols) key_cols <- c(key_cols, "elaborado")  # forecast issue time
  
  cat("Removing duplicates based on:", paste(key_cols, collapse = ", "), "\n")
  combined_forecast <- unique(combined_forecast, by = key_cols)
  
} else {
  combined_forecast <- new_forecast_data
}

# Sort by municipality and date
combined_forecast <- combined_forecast[order(get(muni_col), get(date_col))]

cat("Final forecast dataset:", nrow(combined_forecast), "records\n")
if (nrow(combined_forecast) > 0) {
  cat("Date range:", min(combined_forecast[[date_col]], na.rm = TRUE), 
      "to", max(combined_forecast[[date_col]], na.rm = TRUE), "\n")
  cat("Municipalities:", length(unique(combined_forecast[[muni_col]])), "\n")
}

# Save combined forecast data
fwrite(combined_forecast, existing_forecast_file)

cat("\n=== Municipal Forecast Collection Complete ===\n")
cat("Output file:", existing_forecast_file, "\n")
cat("Records:", nrow(combined_forecast), "\n")
cat("File size:", round(file.size(existing_forecast_file)/1024/1024, 1), "MB\n")

# Show data completeness for key variables
cat("\nData completeness by variable:\n")
temp_vars <- names(combined_forecast)[grepl("temp|tmed|tmax|tmin", names(combined_forecast), ignore.case = TRUE)]
for (var in temp_vars[1:min(5, length(temp_vars))]) {  # Show up to 5 temperature variables
  if (is.numeric(combined_forecast[[var]])) {
    completeness <- round(100 * sum(!is.na(combined_forecast[[var]])) / nrow(combined_forecast), 1)
    cat(sprintf("  %s: %s%% complete\n", var, completeness))
  }
}
