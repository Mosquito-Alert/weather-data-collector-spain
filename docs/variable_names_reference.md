# AEMET Variable Names Reference

This document provides explanations for the original AEMET variable names used in our weather datasets. We preserve original names to maintain data integrity and traceability.

## Station Data Variables (Historical & Current Daily)

### Identifiers & Metadata
| Variable | Description | Notes |
|----------|-------------|-------|
| `indicativo` | Station identifier | Unique AEMET station code |
| `idema` | Alternative station ID | Same as indicativo in some datasets |
| `nombre` | Station name | Full name of weather station |
| `provincia` | Province | Spanish province where station is located |
| `altitud` | Altitude | Station elevation in meters above sea level |
| `fecha` | Date | Observation date (YYYY-MM-DD) |

### Temperature Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `tmed` | Mean temperature | °C | Daily average temperature |
| `tmax` | Maximum temperature | °C | Daily maximum temperature |
| `tmin` | Minimum temperature | °C | Daily minimum temperature |
| `ta` | Air temperature | °C | Instantaneous temperature (hourly data) |
| `tamax` | Max temp (derived) | °C | Maximum from hourly aggregation |
| `tamin` | Min temp (derived) | °C | Minimum from hourly aggregation |

### Precipitation Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `prec` | Precipitation | mm | Daily precipitation total |
| `pcp` | Precipitation (alt) | mm | Alternative name in some datasets |

### Humidity Variables  
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `hrMedia` | Mean relative humidity | % | Daily average |
| `hrMax` | Maximum relative humidity | % | Daily maximum |
| `hrMin` | Minimum relative humidity | % | Daily minimum |
| `hr` | Relative humidity | % | Instantaneous (hourly data) |

### Wind Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `velmedia` | Mean wind speed | km/h | Daily average wind speed |
| `vv` | Wind speed | km/h | Instantaneous wind speed |
| `dir` | Wind direction | degrees | Predominant wind direction |
| `vd` | Wind direction (alt) | degrees | Instantaneous direction |
| `racha` | Wind gust | km/h | Maximum wind gust |

### Pressure Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `presMax` | Maximum pressure | hPa | Daily maximum atmospheric pressure |
| `presMin` | Minimum pressure | hPa | Daily minimum atmospheric pressure |
| `pres` | Pressure | hPa | Instantaneous pressure |

### Solar/Sunshine Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `sol` | Sunshine hours | hours | Daily sunshine duration |

### Time Variables (when extremes occurred)
| Variable | Description | Format | Notes |
|----------|-------------|--------|-------|
| `horatmin` | Time of minimum temperature | HH:MM | When daily minimum occurred |
| `horatmax` | Time of maximum temperature | HH:MM | When daily maximum occurred |
| `horaHrMax` | Time of max humidity | HH:MM | When daily humidity max occurred |
| `horaHrMin` | Time of min humidity | HH:MM | When daily humidity min occurred |
| `horaPresMax` | Time of max pressure | HH:MM | When daily pressure max occurred |
| `horaPresMin` | Time of min pressure | HH:MM | When daily pressure min occurred |
| `horaracha` | Time of wind gust | HH:MM | When maximum gust occurred |

## Hourly Data Variables

### Core Hourly Variables
| Variable | Description | Notes |
|----------|-------------|-------|
| `fint` | Datetime | Full timestamp (YYYY-MM-DD HH:MM:SS) |
| `idema` | Station ID | AEMET station identifier |
| `measure` | Variable type | What is being measured (when in long format) |
| `value` | Measurement value | Numeric value for the measure |

Common `measure` values in hourly data:
- `ta`: Air temperature (°C)
- `hr`: Relative humidity (%)
- `pres`: Atmospheric pressure (hPa)
- `vv`: Wind speed (km/h)
- `vd`: Wind direction (degrees)
- `prec`: Precipitation (mm)

## Municipal Forecast Variables

### Identifiers
| Variable | Description | Notes |
|----------|-------------|-------|
| `municipio` | Municipality ID | AEMET municipal code |
| `municipio_id` | Municipality ID (alt) | Same as municipio |
| `municipio_nombre` | Municipality name | Full municipality name |
| `provincia` | Province | Province name |
| `fecha` | Date | Forecast date |
| `elaborado` | Forecast issued at | When forecast was generated |

### Forecast Weather Variables
| Variable | Description | Units | Notes |
|----------|-------------|-------|-------|
| `temp_max` | Forecast max temperature | °C | Daily maximum forecast |
| `temp_min` | Forecast min temperature | °C | Daily minimum forecast |
| `temp_avg` | Forecast mean temperature | °C | Daily average forecast |
| `humid_max` | Max humidity forecast | % | Daily maximum humidity |
| `humid_min` | Min humidity forecast | % | Daily minimum humidity |
| `wind_speed` | Wind speed forecast | km/h | Forecast wind speed |

## Data Quality & Metadata Variables

### Collection Metadata
| Variable | Description | Notes |
|----------|-------------|-------|
| `collection_timestamp` | When data was collected | System timestamp |
| `data_source` | Source of data | API endpoint used |
| `n_observations` | Number of source records | For aggregated data |
| `first_observation` | First source timestamp | For aggregated data |
| `last_observation` | Last source timestamp | For aggregated data |

### Quality Indicators  
| Variable | Description | Notes |
|----------|-------------|-------|
| `aggregation_method` | How data was aggregated | Description of aggregation rules |

## Important Notes

1. **Temperature Units**: All temperatures are in Celsius (°C)
2. **Precipitation**: Measured in millimeters (mm), accumulated over the period
3. **Wind Speed**: Measured in kilometers per hour (km/h)
4. **Pressure**: Atmospheric pressure in hectopascals (hPa)
5. **Humidity**: Relative humidity as percentage (%)
6. **Time Zones**: All times are in local Spanish time
7. **Missing Values**: Represented as NA in the data
8. **Station Coverage**: Not all stations measure all variables
9. **Forecast vs Observed**: Municipal forecasts are predictions; station data are observations

## Data Sources

- **Historical Daily**: AEMET historical climatological API
- **Current Daily**: Aggregated from AEMET hourly observations
- **Hourly**: AEMET current hourly observation API  
- **Municipal Forecasts**: AEMET municipal forecast API

This reference maintains compatibility with AEMET's official documentation while providing clarity for data users.
