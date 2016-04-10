#!/bin/bash

if [ -f "../data/sf_crime_data.csv" ]; then
   echo "Data already exists. Skipping download."
else
   echo "Downloading data sets."
   wget -O ../data/sf_crime_data.csv https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD
fi

echo "Loading data into HDFS."
hdfs dfs -mkdir /user/w205/sfcrime
hdfs dfs -put ../data/sf_crime_data.csv /user/w205/sfcrime

echo "Creating Hive tables."
hive -f create_tables.sql

