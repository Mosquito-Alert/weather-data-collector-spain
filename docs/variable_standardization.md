# Variable Standardization Documentation

## Overview
This document describes the standardized variable names used across all three weather datasets and their mapping from original AEMET variable names.

## Municipality Code Information
**IMPORTANT**: The municipality_id field uses CUMUN codes from AEMET's municipal forecast system. 
- Source: AEMET OpenData API municipal forecasts
- Format: 5-digit numeric code
- Coverage: All Spanish municipalities (~8,000+)
- Note: This differs from INE codes - use appropriate conversion if merging with other Spanish administrative datasets

## Dataset 1: Daily Station Historical (`daily_station_historical.csv`)

### Weather Variables
| Standard Name | Original AEMET | Description | Units |
|---------------|----------------|-------------|-------|
| temp_mean | tmed | Daily mean temperature | °C |
| temp_max | tmax | Daily maximum temperature | °C |
| temp_min | tmin | Daily minimum temperature | °C |
| precipitation | prec | Daily precipitation | mm |
| humidity_mean | hrMedia | Daily mean relative humidity | % |
| humidity_max | hrMax | Daily maximum relative humidity | % |
| humidity_min | hrMin | Daily minimum relative humidity | % |
| wind_speed | velmedia | Daily mean wind speed | km/h |
| wind_direction | dir | Daily predominant wind direction | degrees |
| wind_gust | racha | Daily maximum wind gust | km/h |
| pressure_max | presMax | Daily maximum pressure | hPa |
| pressure_min | presMin | Daily minimum pressure | hPa |
| solar_hours | sol | Daily sunshine hours | hours |

### Timing Variables
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| time_temp_min | horatmin | Time of minimum temperature | HH:MM |
| time_temp_max | horatmax | Time of maximum temperature | HH:MM |
| time_humidity_max | horaHrMax | Time of maximum humidity | HH:MM |
| time_humidity_min | horaHrMin | Time of minimum humidity | HH:MM |
| time_pressure_max | horaPresMax | Time of maximum pressure | HH:MM |
| time_pressure_min | horaPresMin | Time of minimum pressure | HH:MM |
| time_wind_gust | horaracha | Time of maximum wind gust | HH:MM |

### Identifiers & Metadata
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| station_id | indicativo/idema | Unique station identifier |
| station_name | nombre | Station name |
| province | provincia | Province name |
| altitude | altitud | Station altitude (meters) |
| date | fecha | Date of observation | YYYY-MM-DD |

## Dataset 2: Daily Municipal Extended (`daily_municipal_extended.csv`)

### Weather Variables
| Standard Name | Original AEMET | Description | Units |
|---------------|----------------|-------------|-------|
| temp_mean | temp_avg/tmed_municipal | Daily mean temperature | °C |
| temp_max | temp_max/tmax_municipal | Daily maximum temperature | °C |
| temp_min | temp_min/tmin_municipal | Daily minimum temperature | °C |
| humidity_mean | hrMedia_municipal | Daily mean relative humidity | % |
| humidity_max | humid_max | Daily maximum relative humidity | % |
| humidity_min | humid_min | Daily minimum relative humidity | % |
| wind_speed | wind_speed/velmedia_municipal | Daily mean wind speed | km/h |

### Identifiers & Metadata
| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| municipality_id | municipio_id/municipio/municipio_code | CUMUN municipality code |
| municipality_name | municipio_nombre | Municipality name |
| province | provincia | Province name |
| date | fecha | Date of observation/forecast | YYYY-MM-DD |
| forecast_issued_at | elaborado | When forecast was issued | ISO datetime |
| data_source | data_source/source | 'forecast' or 'station_aggregated' |
| data_priority | priority | Data priority (1=station, 2=forecast) |

## Dataset 3: Hourly Station Ongoing (`hourly_station_ongoing.csv`)

| Standard Name | Original AEMET | Description |
|---------------|----------------|-------------|
| station_id | idema | Unique station identifier |
| datetime | fint | Observation datetime | ISO datetime |
| date | date | Observation date | YYYY-MM-DD |
| variable_type | measure | Type of measurement |
| value | value | Measured value |

## Data Priority Logic

In the municipal dataset, when both forecast and station-aggregated data exist for the same municipality and date:
1. **Station-aggregated data takes precedence** (data_priority = 1)
2. **Forecast data is secondary** (data_priority = 2)

This ensures that actual measurements replace forecasts as they become available.

## Quality Control Variables

- **temp_range_ok**: Temperature range passes basic sanity checks
- **temp_realistic**: Temperature values are realistic for Spain
- **prec_realistic**: Precipitation values are realistic
- **n_stations**: Number of stations used for municipal aggregation

---
Generated on: $(date)

