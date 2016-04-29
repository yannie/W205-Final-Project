## Bike Theft Application for Berkeley W205 Final Project.
Authors: Annie Lee and Roiana Reid

#### These are instructions on how to used the code provided in this repository to replicate our analysis.

##### Setting up the programming environment:

1. Run the "setup_env.sh" script as root user to install Hadoop, Spark SQL and Postgres, set up the Python 2.7 virtual environment and install the necessary Python packages.

##### Data downloading, cleaning and exploration

1. Switch to the w205 user and activate the py27 environment with the following command:  
```
$ source /opt/py27environment/bin/activate
```

2. Run the "setup_data_tables.sh" script from the scripts directory to download the raw crime and bike parking datasets, format the data, load pre-aggregated data and load the final scores into Hive.

3. This step is optional and only necessary if you want to explore the San Francisco crime and bike parking datasets. Run the "explore_sf_data.sql" using the following command:
```
$ hive -f explore_sf_data.sql
```

#### Analytical component, merging of data

1. Run "aggregates_chi.py" to load the Chicago crime and bike parking installments datasets into tables, and calculate total number of crimes for each category in ('MOTOR VEHICLE THEFT', 'THEFT', 'ROBBERY') for each bike rack. Note: depending on the filtering condition set for number of bike racks, this could take multiple hours to run. Filtering for 2 bike racks will take around 20 minutes to run. Results are written out to data/chi_parking_scores/part-0000. Run this script with the following command:  
```
$ spark-submit aggregates_chi.py
```

2. Run "aggregates_sf.py" to load the San Francisco crime and bike parking installments datasets into tables, and calculate total number of crimes for each category in ('VEHICLE THEFT', 'LARCENY/THEFT', 'ROBBERY') for each bike rack. Note: depending on the filtering condition set for number of bike racks, this could take multiple hours to run on a single machine. Filtering for 2 bike racks will take around 10 minutes to run. Results are written out to data/sf_parking_scores/part-0000. Run this script with the following command:
```
$ spark-submit aggregates_sf.py
```

3. Run "upload_scores.sh" to upload the aggregated rank data in Hadoop and to run the scripts used to calculate the safety score for each bike rack location analyzed in Chicago and San Francisco. This is only necessary if you want to upload newly computed scores into Hive. The setup_data_table.sh script from the setup section already loads the pre-computed scores into Hive. 

#### Serving Component with Real Time Processing

1. Start hiveserver2 with the following command:  
```
$ hive --service hiveserver2
```

2. Run "app.py" to start the application interface for accessing the bike parking data. Make sure to first update the script with the public ip address of your EC2 instance so it knows which Hive server to connect to. Then, run with the following command:  
```
$ python app.py
```

3. Issue requests against the REST API with the following example commands by supplying the latitude and longitude:  
```
$ curl http://ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com:8080/sf?lat=37.7749290&long=-122.4194160
$ curl http://ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com:8080/chi?lat=41.8781140&long=-87.6297980
```





