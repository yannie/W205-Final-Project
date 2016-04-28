#!/bin/bash
# Script to load download raw datasets and load final scores into Hive.

echo "Loading San Francisco data sets."
mkdir ../../data
if [ -f "../../data/sf_crime_data.csv" ]; then
   echo "SF crime data already exists. Skipping download."
else
   echo "Downloading sf crime data sets."
   wget -O ../../data/sf_crime_data_raw.csv https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD
fi

if [ -f "../data/sf_bike_data.csv" ]; then
   echo "SF bike parking data already exists. Skipping download."
else
   echo "Downloading sf bike parking data sets."
   wget -O ../../data/sf_bike_parking_data.csv https://data.sfgov.org/api/views/w969-5mn4/rows.csv?accessType=DOWNLOAD
fi

# Format crime data by removing header.
tail -n +2 ../../data/sf_crime_data_raw.csv > ../../data/sf_crime_data.csv
rm ../../data/sf_crime_data_raw.csv

# Format SF bike parking data.
./format_sf_bike_parking_data.sh

echo "Loading data into HDFS."
hdfs dfs -mkdir /user/w205/sfcrime
hdfs dfs -put ../../data/sf_crime_data.csv /user/w205/sfcrime
hdfs dfs -mkdir /user/w205/sfbike
hdfs dfs -put ../data/sf_bike_data.csv /user/w205/sfbike

echo "Loading Chicago data sets."
if [ -f "../../data/chi_crime_data.csv" ]; then
   echo "Chicago crime data already exists. Skipping download."
else
   echo "Downloading Chicago crime data sets."
   wget -O ../../data/chi_crime_data_raw.csv https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
fi

if [ -f "../data/chi_bike_data.csv" ]; then
   echo "SF bike parking data already exists. Skipping download."
else
   echo "Downloading sf bike parking data sets."
   wget -O ../data/chi_bike_data_raw.csv https://data.cityofchicago.org/api/views/cbyb-69xx/rows.csv?accessType=DOWNLOAD
fi

# Remove headers from data.
tail -n +2 ../../data/chi_crime_data_raw.csv > ../../data/chi_crime_data.csv
rm ../../data/chi_crime_data_raw.csv
tail -n +2 ../data/chi_bike_data_raw.csv > ../data/chi_bike_data.csv
rm ../data/chi_bike_data_raw.csv

echo "Loading data into HDFS."
hdfs dfs -mkdir /user/w205/chicrime
hdfs dfs -put ../../data/chi_crime_data.csv /user/w205/chicrime
hdfs dfs -mkdir /user/w205/chibike
hdfs dfs -put ../data/chi_bike_data.csv /user/w205/chibike

echo "Creating Hive tables."
hive -f create_sf_tables.sql

# Load and calculate bike parking scores.
./upload_scores.sh
