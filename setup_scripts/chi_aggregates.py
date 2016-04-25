# Script to process crime and parking data.

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#import pandas
import csv
import io
import re

sc = SparkContext()
sqlContext = SQLContext(sc)

# Load bike parking data.
print "Loading bike parking data."
lines = sc.textFile('file:///home/w205/final_project/W205-Final-Project/data/chi_bike_data.csv')
# Iterates through each line, removes non-ascii characters, and splits on columns (ignores commas in quotes).
parts = lines.map(lambda l: csv.reader(io.StringIO(re.sub(r'[^\x00-\x7f]',r'', l))).next())
bikeParkingData = parts.map(lambda p: (int(p[0]), p[1], int(p[2]), int(p[3]), p[4], int(p[5]), float(p[6]), float(p[7])))
fields = [StructField('rack_id', IntegerType(), True),
          StructField('address', StringType(), True),
          StructField('ward', IntegerType(), True),
          StructField('comm', IntegerType(), True),
          StructField('comm_name', StringType(), True),
          StructField('tot_install', IntegerType(), True),
          StructField('lat', FloatType(), True),
          StructField('long', FloatType(), True)]
schema = StructType(fields)
bikeParkingDF = sqlContext.createDataFrame(bikeParkingData, schema)
filteredBikeParkingDF = bikeParkingDF.filter("rack_id NOT IN (651, 1409) AND tot_install BETWEEN 6 and 7")
filteredBikeParkingDF.registerTempTable('c_bike_parking_data')
results = sqlContext.sql('SELECT * FROM c_bike_parking_data')
results.show()

# Load crime data.
print "Loading crime data."
crimeLines = sc.textFile('file:///home/w205/final_project/data/chi_crime_data.csv')
crimeParts = crimeLines.map(lambda l: csv.reader(io.StringIO(re.sub(r'[^\x00-\x7f]',r'', l))).next())
# Filter data first to get rid of rows with missing values and incidents before 2013.
crimeData = crimeParts.filter(lambda x: len(x)==22 and x[19]!="" and x[20]!="" and int(x[17]) > 2013) \
                      .map(lambda row: (int(row[0]), row[3], row[5], row[6], row[7],
                                        int(row[17]), float(row[19]), float(row[20])))
crimeFields = [StructField('id', IntegerType(), True),
               StructField('block', StringType(), True),
               StructField('primary_type', StringType(), True),
               StructField('description', StringType(), True),
               StructField('loc_desc', StringType(), True),
               StructField('year', IntegerType(), True),
               StructField('lat', FloatType(), True),
               StructField('long', FloatType(), True)]
crimeSchema = StructType(crimeFields)
crimeDF = sqlContext.createDataFrame(crimeData, crimeSchema)
# Filter crime data for only motor vehicle theft, theft and robbery.
filteredCrimeDF = crimeDF.filter("primary_type IN ('MOTOR VEHICLE THEFT', 'THEFT', 'ROBBERY')")
filteredCrimeDF.registerTempTable('c_crime_data')
results = sqlContext.sql('SELECT * FROM c_crime_data')
results.show()

# Create data frame of bike parking .1 mile search boundaries.
parkingCrimeAreasDF = sqlContext.sql('SELECT '
  				     'rack_id, '
                                     'address, '
  				     'lat-(.1/69) as min_lat, '
  				     'lat+(.1/69) as max_lat, '
  				     'long-(.1/abs(cos(lat*pi()/180)*69)) as min_long, '
                                     'long+(.1/abs(cos(lat*pi()/180)*69)) as max_long '
				     'FROM c_bike_parking_data')
parkingCrimeAreasDF.registerTempTable('c_parking_crime_areas')
parkingCrimeAreasDF.show()

# Join parkingCrimeAreas with crime data.
print "Calculating Cartesian product"
cart = sqlContext.sql('SELECT '
                      'p.address as address, '
                      'p.rack_id, '
		      'primary_type, '
		      'lat, long, '
		      'min_lat, max_lat, '
		      'min_long, max_long '
 		      'FROM c_parking_crime_areas AS p, c_crime_data AS c')
cart.show()

# Filter out crimes not within a quarter-mile radius of each bike stop.
print "Filtering data"
filteredCart = cart.filter("lat >= min_lat AND lat <= max_lat AND long >= min_long AND long <= max_long")
filteredCart.show()

# Group data by bikestop to get total number of crimes.
print "Reducing data"
reducedFields = [StructField('address', StringType(), True),
                 StructField('rack_id', StringType(), True),
                 StructField('theft_count', IntegerType(), True),
       		 StructField('robbery_count', IntegerType(), True),
          	 StructField('vehicle_theft_count', IntegerType(), True)]
reducedSchema = StructType(reducedFields)
#reducedDataDict = filteredCart.rdd.map(lambda row: (row[0], row)).countByKey()
# Groups data by address and category. Currently commented out.
#reducedDataDict = filteredCart.rdd.map(lambda row: (','.join((row[0], row[1])), row)).countByKey()

def GetCounts(category):
  if category == 'THEFT':
    return (1, 0, 0)
  elif category == 'ROBBERY':
    return (0, 1, 0)
  elif category == 'MOTOR VEHICLE THEFT':
    return (0, 0, 1)
  else:
    return (0, 0, 0)
reducedData = filteredCart.rdd.map(lambda row: ((row[0], row[1]), GetCounts(row[2]))) \
                              .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                              .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2]))
#print reducedDataDict.collect()
# Convert to dataframe to easily view result.
#reducedData = sc.parallelize(reducedDataDict.items())
reducedDF = sqlContext.createDataFrame(reducedData, reducedSchema)
reducedDF.show()

# Output data to a csv file.
#reducedDF.toPandas().to_csv('file:///home/w205/final_project/W205-Final-Project/data/ch_bike_parking_scores.csv')
#reducedDF.write.format('com.databricks.spark.csv').save('sf_parking_scores.csv')
reducedData.map(lambda x: ",".join(map(str, x))) \
           .coalesce(1) \
           .saveAsTextFile('file:///home/w205/final_project/W205-Final-Project/data/chi_parking_scores')
