#!/usr/bin/env Rscript

# aggregate_daily_stations_current.R
# ----------------------------------
# Purpose: Aggregate the nationwide hourly observation feed into daily metrics
# for the most recent ~4 days (bridging the gap before historical data arrives).

rm(list = ls())

suppressPackageStartupMessages({
	library(data.table)
	library(lubridate)
	library(stringr)
})

input_path <- "data/output/hourly_station_ongoing.csv.gz"
output_path <- "data/output/daily_station_current.csv.gz"

if (!file.exists(input_path)) {
	stop("Hourly dataset not found at ", input_path, ". Run get_latest_data.R first.")
}

hourly <- fread(input_path, showProgress = FALSE)
if (!nrow(hourly)) {
	cat("Hourly dataset empty; nothing to aggregate.\n")
	quit(status = 0)
}

required_cols <- c("fint", "idema", "measure", "value")
missing_cols <- setdiff(required_cols, names(hourly))
if (length(missing_cols)) {
	stop("Hourly dataset is missing columns: ", paste(missing_cols, collapse = ", "))
}

hourly[, fint := as_datetime(fint)]
hourly[, date := as_date(fint)]

# Pivot to wide per timestamp for easier aggregation
wide <- dcast(
	hourly,
	idema + indicativo + nombre + provincia + altitud + municipio_natcode + municipio_name + fint + date ~ measure,
	value.var = "value",
	fun.aggregate = function(x) suppressWarnings(mean(as.numeric(x), na.rm = TRUE)),
	fill = NA_real_
)

# Ensure expected measurement columns exist even if API omits them
expected_measures <- c("ta", "tamax", "tamin", "hr", "prec", "vv", "pres")
missing_measures <- setdiff(expected_measures, names(wide))
if (length(missing_measures)) {
	for (mc in missing_measures) {
		wide[, (mc) := NA_real_]
	}
}

if (!nrow(wide)) {
	cat("Nothing to aggregate after reshaping; exiting.\n")
	quit(status = 0)
}

agg_na <- function(x, fun) {
	res <- suppressWarnings(fun(x, na.rm = TRUE))
	if (is.infinite(res)) NA_real_ else res
}

# Daily summaries per station
summary_daily <- wide[, .(
	tmed = agg_na(ta, mean),
	tmax = agg_na(c(tamax, ta), max),
	tmin = agg_na(c(tamin, ta), min),
	hrMedia = agg_na(hr, mean),
	hrMax = agg_na(hr, max),
	hrMin = agg_na(hr, min),
	prec = agg_na(prec, sum),
	velmedia = agg_na(vv, mean),
	presMax = agg_na(pres, max),
	presMin = agg_na(pres, min),
	n_obs = .N
), by = .(
	fecha = date,
	indicativo = fifelse(nchar(indicativo), indicativo, idema),
	idema,
	nombre,
	provincia,
	altitud,
	municipio_natcode,
	municipio_name
)]

setorder(summary_daily, fecha, indicativo)

if (file.exists(output_path)) {
	existing <- fread(output_path)
	combined <- rbindlist(list(existing, summary_daily), fill = TRUE)
	setorder(combined, fecha, indicativo)
	combined <- unique(combined, by = c("fecha", "indicativo"))
} else {
	combined <- summary_daily
}

fwrite(combined, output_path)

cat("Current-daily dataset updated:", output_path, "\n")
cat("Rows added this run:", nrow(summary_daily), " | Total rows stored:", nrow(combined), "\n")
cat("Date range:", as.character(min(combined$fecha)), "to", as.character(max(combined$fecha)), "\n")
