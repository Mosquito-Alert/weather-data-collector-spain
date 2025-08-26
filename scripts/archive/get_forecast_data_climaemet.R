#!/usr/bin/env Rscript

# Modern forecast collection using climaemet package
# This should be much more reliable and faster than our custom implementation

library(climaemet)
library(dplyr)
library(data.table)

cat("=== AEMET FORECAST COLLECTION (CLIMAEMET PACKAGE) ===\n")
cat("Started at:", format(Sys.time()), "\n")

# Set API key (assumes it's already configured)
# Run once: aemet_api_key("YOUR_API_KEY", install = TRUE)

# Load municipality data 
cat("Loading municipality codes...\n")
municipalities_data = fread("data/input/municipalities.csv.gz")
cat("Loaded", nrow(municipalities_data), "municipalities\n")

# Get all municipality codes
all_municipios = municipalities_data$CUMUN

# Testing mode
TESTING_MODE = FALSE
N_TEST_MUNICIPALITIES = 10

if(TESTING_MODE) {
  all_municipios = head(all_municipios, N_TEST_MUNICIPALITIES)
  cat("Testing with", N_TEST_MUNICIPALITIES, "municipalities\n")
}

cat("Collecting forecasts for", length(all_municipios), "municipalities...\n")

# Use climaemet's built-in function with progress tracking
tryCatch({
  
  # Collect daily forecasts - this handles all the API complexity
  raw_forecasts = aemet_forecast_daily(
    x = all_municipios,
    verbose = FALSE,      # Set to TRUE for debugging
    progress = TRUE       # Show progress bar
  )
  
  cat("Raw forecast collection completed\n")
  cat("Retrieved forecasts for", nrow(raw_forecasts), "municipality-day combinations\n")
  
  # Extract temperature data in tidy format
  cat("Extracting temperature data...\n")
  temperature_data = aemet_forecast_tidy(raw_forecasts, "temperatura")
  
  # Extract humidity data  
  cat("Extracting humidity data...\n")
  humidity_data = aemet_forecast_tidy(raw_forecasts, "humedadRelativa")
  
  # Extract wind data
  cat("Extracting wind data...\n") 
  wind_data = aemet_forecast_tidy(raw_forecasts, "viento")
  
  # Combine into our expected format
  cat("Combining data into standard format...\n")
  final_data = temperature_data %>%
    select(municipio_id = municipio, 
           municipio_nombre = nombre, 
           provincia, 
           elaborado, 
           fecha,
           temp_max = temperatura_maxima,
           temp_min = temperatura_minima) %>%
    left_join(
      humidity_data %>% 
        select(municipio, fecha, humid_max = humedadRelativa_maxima, humid_min = humedadRelativa_minima),
      by = c("municipio_id" = "municipio", "fecha")
    ) %>%
    left_join(
      wind_data %>%
        select(municipio, fecha, wind_speed = viento_velocidad), 
      by = c("municipio_id" = "municipio", "fecha")
    ) %>%
    mutate(
      temp_avg = rowMeans(cbind(temp_max, temp_min), na.rm = TRUE),
      collected_at = Sys.time()
    )
  
  cat("=== RESULTS ===\n")
  cat("Total forecast records:", nrow(final_data), "\n")
  cat("Municipalities with data:", length(unique(final_data$municipio_id)), "\n") 
  cat("Date range:", as.character(min(final_data$fecha)), "to", as.character(max(final_data$fecha)), "\n")
  
  # Save the data
  dir.create("data/output", recursive = TRUE, showWarnings = FALSE)
  output_file = paste0("data/output/municipal_forecasts_", Sys.Date(), ".csv")
  write.csv(final_data, output_file, row.names = FALSE)
  cat("Data saved to:", output_file, "\n")
  
  # Also save compressed version
  fwrite(final_data, paste0(output_file, ".gz"))
  cat("Compressed version saved to:", paste0(output_file, ".gz"), "\n")
  
}, error = function(e) {
  cat("ERROR in forecast collection:", e$message, "\n")
  quit(save = "no", status = 1)
})

cat("Completed at:", format(Sys.time()), "\n")
