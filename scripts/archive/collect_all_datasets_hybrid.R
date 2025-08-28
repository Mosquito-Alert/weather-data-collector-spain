#!/usr/bin/env Rscript

# HYBRID APPROACH: Master script to collect all three datasets efficiently
# 1. Daily station data (means, min, max) - proven custom approach
# 2. Municipal forecasts - fast climaemet approach  
# 3. Hourly data - existing working approach (get_latest_data.R)

cat("========================================\n")
cat("HYBRID WEATHER DATA COLLECTION SYSTEM\n")
cat("========================================\n")
cat("Started at:", format(Sys.time()), "\n\n")

# Configuration
COLLECT_STATION_DAILY = TRUE     # Dataset 1: Daily station means/min/max
COLLECT_MUNICIPAL_FORECASTS = TRUE   # Dataset 2: Municipal + 7-day forecasts 
COLLECT_HOURLY_DATA = TRUE       # Dataset 3: Hourly for history building

# Timing tracking
start_time = Sys.time()
times = list()

if(COLLECT_STATION_DAILY) {
  cat("=== DATASET 1: DAILY STATION DATA ===\n")
  cat("Collecting daily means, minimums, and maximums by weather station\n")
  
  dataset1_start = Sys.time()
  
  tryCatch({
    source("code/get_station_daily_hybrid.R")
    dataset1_end = Sys.time()
    times$station_daily = as.numeric(difftime(dataset1_end, dataset1_start, units = "mins"))
    cat("✅ Dataset 1 completed in", round(times$station_daily, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 1 failed:", e$message, "\n\n")
    times$station_daily = NA
  })
}

if(COLLECT_MUNICIPAL_FORECASTS) {
  cat("=== DATASET 2: MUNICIPAL FORECASTS ===\n")
  cat("Collecting municipal data with 7-day forecasts using climaemet\n")
  
  dataset2_start = Sys.time()
  
  tryCatch({
    source("code/get_forecast_data_hybrid.R")
    dataset2_end = Sys.time()
    times$municipal_forecasts = as.numeric(difftime(dataset2_end, dataset2_start, units = "mins"))
    cat("✅ Dataset 2 completed in", round(times$municipal_forecasts, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 2 failed:", e$message, "\n\n")
    times$municipal_forecasts = NA
  })
}

if(COLLECT_HOURLY_DATA) {
  cat("=== DATASET 3: HOURLY DATA ===\n")
  cat("Collecting hourly data for building history\n")
  
  dataset3_start = Sys.time()
  
  tryCatch({
    source("code/get_latest_data.R")
    dataset3_end = Sys.time()
    times$hourly_data = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    cat("✅ Dataset 3 completed in", round(times$hourly_data, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    dataset3_end = Sys.time()
    dataset3_end = Sys.time()
    times$hourly_data = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
    cat("✅ Dataset 3 completed in", round(times$hourly_data, 2), "minutes\n\n")
  }, error = function(e) {
    cat("❌ Dataset 3 failed:", e$message, "\n\n")
    dataset3_end = Sys.time()
    times$hourly_data = as.numeric(difftime(dataset3_end, dataset3_start, units = "mins"))
  })
}

# === POST-COLLECTION GAP ANALYSIS AND MONITORING ===
cat("=== POST-COLLECTION ANALYSIS ===\n")

# Run gap analysis
cat("Running gap analysis...\n")
tryCatch({
  source("code/check_data_gaps.R")
  cat("✅ Gap analysis completed.\n")
}, error = function(e) {
  cat("❌ Gap analysis failed:", e$message, "\n")
})

# Update data summary
cat("Updating data summary...\n")
tryCatch({
  source("code/generate_data_summary.R")
  cat("✅ Data summary updated.\n")
}, error = function(e) {
  cat("❌ Data summary failed:", e$message, "\n")
})

# Update README with current status
cat("Updating README with latest data status...\n")
tryCatch({
  source("code/update_readme_with_summary.R")
  cat("✅ README updated with current data status.\n")
}, error = function(e) {
  cat("❌ README update failed:", e$message, "\n")
})

# Final summary
end_time = Sys.time()
total_time = as.numeric(difftime(end_time, start_time, units = "mins"))

cat("========================================\n")
cat("COLLECTION SUMMARY\n")
cat("========================================\n")
cat("Total execution time:", round(total_time, 2), "minutes\n\n")

cat("Individual dataset times:\n")
if(exists("times") && !is.null(times$station_daily) && !is.na(times$station_daily)) {
  cat("  Dataset 1 (Station Daily):", round(times$station_daily, 2), "minutes\n")
}
if(exists("times") && !is.null(times$municipal_forecasts) && !is.na(times$municipal_forecasts)) {
  cat("  Dataset 2 (Municipal Forecasts):", round(times$municipal_forecasts, 2), "minutes\n")
}
if(exists("times") && !is.null(times$hourly_data) && !is.na(times$hourly_data)) {
  cat("  Dataset 3 (Hourly Data):", round(times$hourly_data, 2), "minutes\n")
}

cat("\nEstimated improvement over previous approach:\n")
if(exists("times") && !is.null(times$municipal_forecasts) && !is.na(times$municipal_forecasts)) {
  old_forecast_time = 33 * 60  # 33 hours in minutes
  improvement = old_forecast_time / times$municipal_forecasts
  cat("  Municipal forecasts: ~", round(improvement, 1), "x faster\n")
  cat("  Time saved: ~", round((old_forecast_time - times$municipal_forecasts) / 60, 1), "hours\n")
}

cat("\nOutput files generated:\n")
files_to_check = c(
  paste0("data/output/station_daily_data_", Sys.Date(), ".csv"),
  paste0("data/output/municipal_forecasts_", Sys.Date(), ".csv"),
  paste0("data/output/latest_weather_", Sys.Date(), ".csv")
)

for(file in files_to_check) {
  if(file.exists(file)) {
    file_size = round(file.size(file) / 1024 / 1024, 2)
    cat("  ✅", file, "(", file_size, "MB )\n")
  } else {
    cat("  ❌", file, "(not found)\n")
  }
}

cat("\nCompleted at:", format(Sys.time()), "\n")
