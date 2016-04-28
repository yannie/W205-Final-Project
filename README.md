# W205-Final-Project
## Bike Theft Application for Berkeley W205 Final Project.

#### These are instructions on how to used the code provided in this repository to replicate our analysis.

##### Setting up the programming environment:

1. Run the "setup_env.sh" script in order to install Hadoop, Spark SQL and Postgres, set up the Python 2.7 virtual environment and install the necessary Python packages.

##### Data downloading, cleaning and exploration

1. Run the "setup_data_tables.sh" script to download the raw crime and parking datasets, format the data, load pre-aggregated data and load the final scores into Hive.

2. This step is optional and only necessary if you want to explore the San Francisco crime and bike parking datasets. Run "create_sf_tables.sql".

#### Analytical component, merging of data

1. Run "aggregates_chi.py" to load the Chicago crime and bike parking installments datasets into tables, and calculate ranks of each category of crime ('MOTOR VEHICLE THEFT', 'THEFT', 'ROBBERY') for each bike rack.

2. Run "aggregates_sf.py" to load the San Francisco crime and bike parking installments datasets into tables, and calculate ranks of each category of crime ('VEHICLE THEFT', 'LARCENY/THEFT', 'ROBBERY') for each bike rack.

3. Run "Upload_scores.sh" to upload the aggregated rank data in Haddop and to run the scripts used to calculate the safety score for each bike rack location analyzed in Chicago and San Francisco. 

#### Serving Component with Real Time Processing

1. Run "app.py" to create the application interface for accessing the bike parking data.








