# Quickstart script to load precomputed data necessary for the application into Hive.

echo "Loading data into Hadoop."
hdfs dfs -mkdir /user/w205/sfbike
hdfs dfs -put ../data/sf_bike_data.csv /user/w205/sfbike
hdfs dfs -mkdir /user/w205/chibike
hdfs dfs -put ../data/chi_bike_data.csv /user/w205/chibike

echo "Running upload_scores.sh script."
./upload_scores.sh
