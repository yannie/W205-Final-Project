#!/bin/bash

# Script to clean up SF bike parking data.
# Remove header line.
tail -n +2 ../../data/sf_bike_parking_data.csv > ../data/sf_bike_parking.csv
# Remove every other line break.
awk 'ORS=NR%2?FS:RS' ../data/sf_bike_parking.csv > ../data/sf_bike_data.csv
# Format X and Y values in single location coordinate.
sed -i.bak 's/\"[^(\"]\+[(]\+//g' ../data/sf_bike_data.csv
sed -i.bak 's/[)]\"//g' ../data/sf_bike_data.csv
# Clean up temporary files.
rm ../../data/sf_bike_parking_data.csv
rm ../data/sf_bike_parking.csv
rm ../data/sf_bike_data.csv.bak
