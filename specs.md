# Specifications

This project should produce three data sets:

1. Daily means, minimums, and maxima (as well as total daily precipitation), by weather station, going back as far as the API goes (which I think is 2013) and up through the most recent complete (i.e. yesterday). I am pretty sure that will mean using the historical endpoint to get all data up to 4-days ago (where it stops, unless I am mistaken), using the current hourly data to generate daily aggregated values for the past 4 days. 

2. The same thing, but aggregated by municipality and going up through the 7-day forecast. (I say aggregated by municipality because I understand that the forecast data is by municipality not station).

3. A separate dataset of the hourly data so that we build up our own history of hourly data.