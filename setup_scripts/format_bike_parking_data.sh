#!/bin/bash

# Script to clean up bike parking data.
# Remove header line.
tail -n +2 sf_bike_parking_data.csv > sf_bike_parking.csv
# Remove every other line break.
awk 'ORS=NR%2?FS:RS' sf_bike_parking.csv > sf_bike_data.csv
# Format X and Y values in single location coordinate.
sed -i.bak 's/\"[^(\"]\+[(]\+//g' ../data/sf_bike_data.csv
sed -i.bak 's/[)]\"//g' sf_bike_data.csv
# Clean up temporary files.
rm sf_bike_parking_data.csv
rm sf_bike_parking.csv
rm sf_bike_data.csv.bak
