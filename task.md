## Description

When our researchers are in the field they often collect data with depth information/measurements.
These depth measurements are simply taken by the researcher/field technician in the field, and they are
thus not adjusted and referenced to chart datum (Norwegian “sjøkartnull”).
The data is usually punched in manually in an MS Excel spreadsheet for further processing (typically one
row pr. observation), etc.

## Task

Using the supplied Excel file (tidal_sample.xlsx) with depth data (one observation pr. row) you should write a script
in a language of your choice which:
- Reads the excel file with sample data
- relevant columns:
  - Date
  - Time
  - GPS Latitude
  - GPS Longitude
  - depth
- Use the API from the Norwegian mapping authority (Kartverket) to find the water level for each observation with reference to chart datum (sjøkartnull)
- Calculate chart datum (sjøkartnull) referenced sampling depth for each data row in the spreadsheet
- Return the result in an appropriate format for further processing
  - For yourself
  - For the researcher producing the data

The API for tidal water level adjustment is described here: http://api.sehavniva.no/tideapi_en.html

You can assume the following:
  - All time stamps are from the Norwegian time zone (i.e. "Europe/Oslo" including daylight saving time).
  - The coordinates are in WGS84 coordinate system (standard for GPS)