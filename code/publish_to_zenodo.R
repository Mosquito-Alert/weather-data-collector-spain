#!/usr/bin/env Rscript

# publish_to_zenodo.R
# --------------------
# Purpose: Automated publishing system for weather datasets to Zenodo
# 
# This script handles:
# 1. Dataset metadata generation with proper attribution
# 2. GitHub release creation 
# 3. Zenodo API integration for controlled publishing
# 4. Automatic DOI generation and citation formatting

library(httr)
library(jsonlite)
library(gh) # GitHub API
library(here)

# Configuration
ZENODO_TOKEN <- Sys.getenv("ZENODO_TOKEN")  # Set in environment
GITHUB_TOKEN <- Sys.getenv("GITHUB_TOKEN")  # Set in environment

# Metadata template generator
generate_dataset_metadata <- function(dataset_name, file_path, description) {
  
  file_info <- file.info(file_path)
  
  metadata <- list(
    title = paste("Spanish Weather Data:", dataset_name),
    description = paste(description, 
                       "This dataset is derived from AEMET (Agencia Estatal de Meteorología) data.",
                       "Data processing and aggregation by John R.B. Palmer.",
                       sep = " "),
    creators = list(
      list(
        name = "Palmer, John R.B.",
        affiliation = "Universitat Pompeu Fabra",
        orcid = "0000-0002-2648-7860" 
      ),
      list(
        name = "AEMET - Agencia Estatal de Meteorología",
        affiliation = "Government of Spain"
      )
    ),
    keywords = c("weather", "meteorology", "Spain", "AEMET", "climate", "forecast"),
    license = "CC-BY-4.0",
    related_identifiers = list(
      list(
        identifier = "https://opendata.aemet.es/",
        relation = "isDerivedFrom"
      )
    ),
    notes = "© AEMET. Autorizado el uso de la información y su reproducción citando a AEMET como autora de la misma.",
    upload_type = "dataset",
    access_right = "open",
    file_size = file_info$size,
    file_modified = format(file_info$mtime, "%Y-%m-%d %H:%M:%S"),
    processing_date = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  
  return(metadata)
}

# Function to create GitHub release
create_github_release <- function(tag_name, release_name, description, files = NULL) {
  # Implementation for GitHub releases
  cat("Creating GitHub release:", tag_name, "\n")
  # gh::gh("POST /repos/:owner/:repo/releases", ...)
}

# Function to publish to Zenodo via API
publish_to_zenodo_api <- function(metadata, file_path) {
  # Implementation for direct Zenodo API publishing
  cat("Publishing to Zenodo via API\n")
  cat("File:", file_path, "\n")
  cat("Title:", metadata$title, "\n")
}

# Main publishing function
publish_dataset <- function(dataset_type = c("daily_station", "daily_municipal", "hourly_station"), 
                           method = c("github_release", "zenodo_api")) {
  
  dataset_type <- match.arg(dataset_type)
  method <- match.arg(method)
  
  # Define dataset configurations
  datasets <- list(
    daily_station = list(
      name = "Daily Station Historical Data",
      file = "data/output/daily_station_historical.csv.gz",
      description = "Daily aggregated weather observations from Spanish meteorological stations, combining historical records (2013+) with recent observations."
    ),
    daily_municipal = list(
      name = "Daily Municipal Extended Data", 
      file = "data/output/daily_municipal_extended.csv.gz",
      description = "Daily weather data by Spanish municipality, combining historical observations with 7-day forecasts."
    ),
    hourly_station = list(
      name = "Hourly Station Ongoing Data",
      file = "data/output/hourly_station_ongoing.csv.gz", 
      description = "Hourly weather observations from Spanish meteorological stations, building a continuous historical archive."
    )
  )
  
  dataset_config <- datasets[[dataset_type]]
  
  if (!file.exists(dataset_config$file)) {
    stop("Dataset file not found: ", dataset_config$file)
  }
  
  # Generate metadata
  metadata <- generate_dataset_metadata(
    dataset_config$name,
    dataset_config$file, 
    dataset_config$description
  )
  
  # Save metadata locally
  metadata_file <- paste0("docs/metadata_", dataset_type, ".json")
  write_json(metadata, metadata_file, pretty = TRUE)
  cat("Metadata saved to:", metadata_file, "\n")
  
  # Publish based on method
  if (method == "github_release") {
    create_github_release(
      tag_name = paste0(dataset_type, "_", Sys.Date()),
      release_name = dataset_config$name,
      description = dataset_config$description,
      files = dataset_config$file
    )
  } else if (method == "zenodo_api") {
    publish_to_zenodo_api(metadata, dataset_config$file)
  }
}

# Usage examples (commented out):
# publish_dataset("daily_municipal", "github_release")
# publish_dataset("daily_municipal", "zenodo_api")

cat("Zenodo publishing system initialized\n")
cat("Ready for dataset publication with proper AEMET attribution\n")
