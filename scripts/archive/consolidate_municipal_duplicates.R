#!/usr/bin/env Rscript

# Proper consolidation of duplicate columns in municipal dataset
# This script examines the content of duplicate columns and consolidates them intelligently

library(data.table)
library(dplyr)

cat("=== FIXING MUNICIPAL DATASET DUPLICATE COLUMNS ===\n")

# Load the data with fread to see actual column structure
data <- fread("data/output/daily_municipal_extended.csv")
cat("Original data: ", nrow(data), "rows, ", ncol(data), "columns\n")

# Get original column names before any processing
original_cols <- colnames(data)
cat("Original columns:\n")
print(original_cols)

# Create a clean version by examining each type of duplicate

# 1. MUNICIPALITY ID CONSOLIDATION
cat("\n=== CONSOLIDATING MUNICIPALITY ID ===\n")
muni_id_positions <- which(original_cols == "municipality_id")
cat("municipality_id appears at positions:", paste(muni_id_positions, collapse=", "), "\n")

if (length(muni_id_positions) > 1) {
  # Examine content of each municipality_id column
  for (i in seq_along(muni_id_positions)) {
    pos <- muni_id_positions[i]
    col_data <- data[[pos]]
    non_na_count <- sum(!is.na(col_data))
    sample_vals <- unique(col_data[!is.na(col_data)])[1:5]
    cat("  Position", pos, ": ", non_na_count, "non-NA values, sample:", paste(sample_vals, collapse=", "), "\n")
  }
  
  # Keep the column with most non-NA values, or first if tied
  best_muni_col <- muni_id_positions[1]
  for (pos in muni_id_positions) {
    if (sum(!is.na(data[[pos]])) > sum(!is.na(data[[best_muni_col]]))) {
      best_muni_col <- pos
    }
  }
  
  cat("  Keeping municipality_id from position", best_muni_col, "\n")
  
  # Create new clean dataset starting with this column
  clean_data <- data.frame(municipality_id = data[[best_muni_col]])
} else {
  clean_data <- data.frame(municipality_id = data[["municipality_id"]])
}

# 2. ADD OTHER UNIQUE COLUMNS (non-duplicated ones)
cat("\n=== ADDING UNIQUE COLUMNS ===\n")
unique_cols <- original_cols[!original_cols %in% c("municipality_id", "temp_mean")]
for (col in unique_cols) {
  if (col %in% original_cols) {
    clean_data[[col]] <- data[[col]]
    cat("  Added:", col, "\n")
  }
}

# 3. TEMP_MEAN CONSOLIDATION  
cat("\n=== CONSOLIDATING TEMP_MEAN ===\n")
temp_mean_positions <- which(original_cols == "temp_mean")
cat("temp_mean appears at positions:", paste(temp_mean_positions, collapse=", "), "\n")

if (length(temp_mean_positions) > 1) {
  # Examine content of each temp_mean column
  for (i in seq_along(temp_mean_positions)) {
    pos <- temp_mean_positions[i]
    col_data <- data[[pos]]
    non_na_count <- sum(!is.na(col_data))
    sample_vals <- unique(col_data[!is.na(col_data)])[1:5]
    cat("  Position", pos, ": ", non_na_count, "non-NA values, sample:", paste(sample_vals, collapse=", "), "\n")
  }
  
  # Keep the column with most non-NA values
  best_temp_col <- temp_mean_positions[1]
  for (pos in temp_mean_positions) {
    if (sum(!is.na(data[[pos]])) > sum(!is.na(data[[best_temp_col]]))) {
      best_temp_col <- pos
    }
  }
  
  cat("  Keeping temp_mean from position", best_temp_col, "\n")
  clean_data$temp_mean <- data[[best_temp_col]]
} else if (length(temp_mean_positions) == 1) {
  clean_data$temp_mean <- data[[temp_mean_positions[1]]]
}

# 4. HANDLE REMAINING UNMAPPED COLUMNS
cat("\n=== MAPPING REMAINING COLUMNS ===\n")
# Map remaining municipal-specific column names to standard names
remaining_mappings <- list(
  "tmax_municipal" = "temp_max",
  "tmin_municipal" = "temp_min", 
  "velmedia_municipal" = "wind_speed",
  "hrMedia_municipal" = "humidity_mean"
)

for (old_name in names(remaining_mappings)) {
  new_name <- remaining_mappings[[old_name]]
  if (old_name %in% colnames(clean_data)) {
    # If target column doesn't exist or is empty, use this one
    if (!new_name %in% colnames(clean_data) || all(is.na(clean_data[[new_name]]))) {
      clean_data[[new_name]] <- clean_data[[old_name]]
      clean_data[[old_name]] <- NULL  # Remove old column
      cat("  Mapped:", old_name, "->", new_name, "\n")
    } else {
      cat("  Skipped:", old_name, "- target", new_name, "already exists with data\n")
    }
  }
}

# 5. FINAL COLUMN ORGANIZATION
cat("\n=== FINAL ORGANIZATION ===\n")

# Define the proper order according to documentation
standard_order <- c(
  "municipality_id", "municipality_name", "province", "date",
  "temp_mean", "temp_max", "temp_min", 
  "humidity_mean", "humidity_max", "humidity_min",
  "wind_speed", "forecast_issued_at", "data_source", "data_priority",
  "collection_timestamp", "processing_timestamp",
  "qc_temp_range", "qc_temp_realistic", "n_stations", "source"
)

# Reorder columns - put standard ones first, then any extras
available_standard <- intersect(standard_order, colnames(clean_data))
extra_cols <- setdiff(colnames(clean_data), standard_order)

final_order <- c(available_standard, extra_cols)
clean_data <- clean_data[, final_order, drop = FALSE]

cat("Final dataset: ", nrow(clean_data), "rows, ", ncol(clean_data), "columns\n")
cat("Final columns:\n")
print(colnames(clean_data))

# Check for any remaining numbered columns
numbered_cols <- grep("\\.[0-9]+$", colnames(clean_data), value = TRUE)
if (length(numbered_cols) > 0) {
  cat("❌ WARNING: Still have numbered columns:", paste(numbered_cols, collapse = ", "), "\n")
} else {
  cat("✅ No numbered columns remaining\n")
}

# 6. SAVE CLEANED VERSION
cat("\n=== SAVING CLEANED DATA ===\n")

# Create backup
backup_file <- paste0("data/output/daily_municipal_extended.csv.backup_before_consolidation_", 
                     format(Sys.time(), "%Y%m%d_%H%M%S"))
file.copy("data/output/daily_municipal_extended.csv", backup_file)
cat("Backup created:", basename(backup_file), "\n")

# Write clean version
write.csv(clean_data, "data/output/daily_municipal_extended.csv", row.names = FALSE)
cat("✅ Cleaned municipal dataset saved\n")

cat("\n🎯 MUNICIPAL DATASET CONSOLIDATION COMPLETE\n")
