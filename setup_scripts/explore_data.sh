#!/bin/bash

if [ -f "../data/sf_crime_data.csv" ]; then
   echo "SF crime data already exists. Skipping download."
else
   echo "Downloading sf crime data sets."
   wget -O ../data/sf_crime_data.csv https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD
fi

if [ -f "../data/sf_bike_parking_data.csv" ]; then
   echo "SF bike parking data already exists. Skipping download."
else
   echo "Downloading sf bike parking data sets."
   wget -O ../data/sf_bike_parking_data.csv https://data.sfgov.org/api/views/w969-5mn4/rows.csv?accessType=DOWNLOAD
fi

echo "Loading data into HDFS."
hdfs dfs -mkdir /user/w205/sfcrime
hdfs dfs -put ../data/sf_crime_data.csv /user/w205/sfcrime
hdfs dfs -mkdir /user/w205/sfbike
hdfs dfs -put ../data/sf_bike_parking_data.csv /user/w205/sfbike

echo "Creating Hive tables."
hive -f create_tables.sql

