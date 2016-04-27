#!/bin/bash
# Script for uploading aggregate data into Hive and
# computing scores for each bike rack.

echo "Transferring scores to Hadoop"
hdfs dfs -mkdir /user/w205/sfscore
hdfs dfs -mkdir /user/w205/chiscore
hdfs dfs -put ../data/sf_parking_scores.csv /user/w205/sfscore
hdfs dfs -put ../data/chi_parking_scores.csv /user/w205/chiscore

echo "Computing and creating score tables in Hive."
hive -f calculate_score_sf.sql
hive -f calculate_score_chi.sql
