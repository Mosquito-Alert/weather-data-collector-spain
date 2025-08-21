# Debug script to understand data structure
library(tidyverse)
library(lubridate) 
library(data.table)

# Check what we get from hourly data
cat("=== CHECKING HOURLY DATA STRUCTURE ===\n")
hourly_recent <- fread("data/output/hourly_station_ongoing.csv.gz")
cat("Hourly data columns:", paste(names(hourly_recent), collapse = ", "), "\n")
cat("Hourly data sample:\n")
print(head(hourly_recent, 5))

cat("\n=== CONVERTING TO WIDE FORMAT ===\n")
hourly_recent[, date := as_date(fint)]

# Convert from long to wide format
hourly_wide <- hourly_recent %>%
  pivot_wider(names_from = measure, values_from = value) %>%
  as.data.table()

cat("Wide format columns:", paste(names(hourly_wide), collapse = ", "), "\n")
cat("Wide format sample:\n")
print(head(hourly_wide, 3))

# Check what variables we have
available_vars <- intersect(names(hourly_wide), c("ta", "tamax", "tamin", "hr", "prec", "vv", "pres"))
cat("\nAvailable target variables:", paste(available_vars, collapse = ", "), "\n")
