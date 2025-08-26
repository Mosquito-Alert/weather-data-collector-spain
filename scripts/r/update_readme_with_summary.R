#!/usr/bin/env Rscript

# update_readme_with_summary.R
# Automatically updates README.md with current data summary
# Can be run manually or as part of collection pipeline

library(tidyverse)

cat("=== UPDATING README WITH DATA SUMMARY ===\n")

# Generate fresh data summary
cat("Generating current data summary...\n")

# Create summary directly
data_dir <- "data/output"
datasets <- c("daily_station_historical.csv", "daily_municipal_extended.csv", "hourly_station_ongoing.csv")

summary_lines <- c(
  "## Current Data Status",
  "",
  paste("*Last updated:", Sys.time(), "*"),
  ""
)

for (dataset in datasets) {
  file_path <- file.path(data_dir, dataset)
  if (file.exists(file_path)) {
    tryCatch({
      data <- read.csv(file_path, nrows = 1)
      file_info <- file.info(file_path)
      summary_lines <- c(summary_lines,
        paste("### ", gsub("_", " ", gsub(".csv", "", dataset))),
        paste("- **Records**: ", nrow(read.csv(file_path, header = TRUE))),
        paste("- **Variables**: ", ncol(data)),
        paste("- **Last Modified**: ", file_info$mtime),
        paste("- **File Size**: ", round(file_info$size / 1024^2, 2), "MB"),
        ""
      )
    }, error = function(e) {
      summary_lines <<- c(summary_lines,
        paste("### ", gsub("_", " ", gsub(".csv", "", dataset))),
        "- **Status**: Error reading file",
        ""
      )
    })
  } else {
    summary_lines <- c(summary_lines,
      paste("### ", gsub("_", " ", gsub(".csv", "", dataset))),
      "- **Status**: File not found",
      ""
    )
  }
}

new_summary <- summary_lines
cat("Read", length(new_summary), "lines from summary file.\n")

# Read current README.md
readme_file = "README.md"
if(!file.exists(readme_file)) {
  cat("ERROR: README.md not found.\n")
  quit(save = "no", status = 1)
}

readme_lines = readLines(readme_file)
cat("Read", length(readme_lines), "lines from README.md.\n")

# Find insertion point or existing summary section
summary_start_marker = "## 📊 Current Data Collection Status"
summary_end_marker = "---"

# Look for existing summary section
start_idx = which(str_detect(readme_lines, fixed(summary_start_marker)))
end_idx = NULL

if(length(start_idx) > 0) {
  # Find the end of the existing summary section
  end_candidates = which(str_detect(readme_lines[start_idx[1]:length(readme_lines)], fixed(summary_end_marker)))
  if(length(end_candidates) > 0) {
    end_idx = start_idx[1] + end_candidates[1] - 1
  }
}

if(length(start_idx) > 0 && !is.null(end_idx)) {
  # Replace existing summary section
  cat("Found existing summary section at lines", start_idx[1], "to", end_idx, "\n")
  cat("Replacing with updated summary...\n")
  
  updated_readme = c(
    readme_lines[1:(start_idx[1] - 1)],   # Content before summary
    new_summary,                           # New summary
    readme_lines[(end_idx + 1):length(readme_lines)]  # Content after summary
  )
  
} else {
  # Insert new summary section (after performance improvements section if it exists)
  performance_idx = which(str_detect(readme_lines, "Performance Improvements"))
  
  if(length(performance_idx) > 0) {
    # Find end of performance section
    insert_idx = performance_idx[1] + 10  # Insert after performance section
  } else {
    # Insert after the main title
    title_idx = which(str_detect(readme_lines, "^# Weather Data Collector"))
    insert_idx = ifelse(length(title_idx) > 0, title_idx[1] + 2, 5)
  }
  
  cat("Inserting new summary section at line", insert_idx, "\n")
  
  updated_readme = c(
    readme_lines[1:(insert_idx - 1)],
    "",
    new_summary,
    "",
    readme_lines[insert_idx:length(readme_lines)]
  )
}

# Write updated README.md
writeLines(updated_readme, readme_file)
cat("✅ README.md updated successfully.\n")

# Create backup
backup_file = paste0("README_backup_", Sys.Date(), ".md")
writeLines(readme_lines, backup_file)
cat("Original README.md backed up to:", backup_file, "\n")

cat("README.md now includes current data collection status.\n")
