#!/bin/bash

hdfs dfs -mkdir /user/w205/parkingspaces
hdfs dfs -put ../data/sf_parking_scores.csv /user/w205/parkingspaces

hive -f upload_scores.sql
