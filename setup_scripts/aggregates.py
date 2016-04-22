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
lines = sc.textFile('file:///home/w205/final_project/W205-Final-Project/data/sf_bike_data.csv')
# Iterates through each line, removes non-ascii characters, and splits on columns (ignores commas in quotes).
parts = lines.map(lambda l: csv.reader(io.StringIO(re.sub(r'[^\x00-\x7f]',r'', l))).next())
bikeParkingData = parts.map(lambda p: (p[0], p[1], p[2], int(p[3]), int(p[4]), p[5], p[6], p[7], float(p[8]), float(p[9]))).take(2)
fields = [StructField('address', StringType(), True),
          StructField('location_name', StringType(), True),
          StructField('street_name', StringType(), True),
          StructField('racks', IntegerType(), True),
          StructField('spaces', IntegerType(), True),
          StructField('placement', StringType(), True),
          StructField('mo_installed', StringType(), True),
          StructField('yr_installed', StringType(), True),
          StructField('lat', FloatType(), True),
          StructField('long', FloatType(), True)]
schema = StructType(fields)
bpDF = sqlContext.createDataFrame(bikeParkingData, schema)
bpDF.registerTempTable('bike_parking_data')
results = sqlContext.sql('SELECT * FROM bike_parking_data')
results.show()

# Load crime data.
print "Loading crime data."
crimeLines = sc.textFile('file:///home/w205/final_project/data/sf_crime_data.csv')
crimeParts = crimeLines.map(lambda l: csv.reader(io.StringIO(re.sub(r'[^\x00-\x7f]',r'', l))).next())
crimeData = crimeParts.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], float(p[9]), float(p[10])))
crimeFields = [StructField('incident_num', StringType(), True),
          StructField('category', StringType(), True),
          StructField('description', StringType(), True),
          StructField('day_of_week', StringType(), True),
          StructField('date', StringType(), True),
          StructField('time', StringType(), True),
          StructField('pd_district', StringType(), True),
          StructField('resolution', StringType(), True),
	  StructField('address', StringType(), True),
          StructField('long', FloatType(), True),
          StructField('lat', FloatType(), True)]
crimeSchema = StructType(crimeFields)
crimeDF = sqlContext.createDataFrame(crimeData, crimeSchema)
# Filter crime data for only vehicle theft, larceny/theft and robbery
filteredCrimeDF = crimeDF.filter("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date, 'MM/dd/yyyy'), 'yyyy-MM-dd')) > cast('2013-01-01' AS date) AND category IN ('VEHICLE THEFT', 'LARCENY/THEFT', 'ROBBERY')")
filteredCrimeDF.registerTempTable('crime_data')
results = sqlContext.sql('SELECT * FROM crime_data')
results.show()

# Create data frame of bike parking .1 mile search boundaries.
parkingCrimeAreasDF = sqlContext.sql('SELECT '
  				     'address, '
  				     'lat-(.1/69) as min_lat, '
  				     'lat+(.1/69) as max_lat, '
  				     'long-(.1/abs(cos(lat*pi()/180)*69)) as min_long, '
                                     'long+(.1/abs(cos(lat*pi()/180)*69)) as max_long '
				     'FROM bike_parking_data')
parkingCrimeAreasDF.registerTempTable('parking_crime_areas')
parkingCrimeAreasDF.show()

# Join parkingCrimeAreas with crime data.
print "Calculating Cartesian product"
cart = sqlContext.sql('SELECT '
                      'p.address as address, '
		      'category, '
		      'lat, long, '
		      'min_lat, max_lat, '
		      'min_long, max_long '
 		      'FROM parking_crime_areas AS p, crime_data AS c')
cart.show()

# Filter out crimes not within a quarter-mile radius of each bike stop.
print "Filtering data"
filteredCart = cart.filter("lat >= min_lat AND lat <= max_lat AND long >= min_long AND long <= max_long")
filteredCart.show()

# Group data by bikestop to get total number of crimes.
print "Reducing data"
reducedFields = [StructField('address,category', StringType(), True),
          	 StructField('count', IntegerType(), True)]
reducedSchema = StructType(reducedFields)
#reducedDataDict = filteredCart.rdd.map(lambda row: (row[0], row)).countByKey()
# Groups data by address and category. Currently commented out.
#reducedDataDict = filteredCart.rdd.map(lambda row: (','.join((row[0], row[1])), row)).countByKey()

def GetCounts(category):
  if category == 'LARCENY/THEFT':
    return (1, 0, 0)
  elif category == 'ROBBERY':
    return (0, 1, 0)
  elif category == 'VEHICLE THEFT':
    return (0, 0, 1)
  else:
    return (0, 0, 0)
reducedDataDict = filteredCart.rdd.map(lambda row: (row[0], GetCounts(row[1]))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
print reducedDataDict.collect()
# Convert to dataframe to easily view result.
#reducedData = sc.parallelize(reducedDataDict.items())
#reducedDF = sqlContext.createDataFrame(reducedData, reducedSchema)
#reducedDF.show()

# Output data to a csv file.
#reducedDF.toPandas().to_csv('file:///home/w205/final_project/W205-Final-Project/data/sf_bike_parking_scores.csv')
#reducedData.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile('file:///home/w205/final_project/W205-Final-Project/data/sf_parking_scores')
