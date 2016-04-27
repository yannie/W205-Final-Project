DROP TABLE chi_bike_parking_base;
CREATE EXTERNAL TABLE chi_bike_parking_base ( 
  rack_id int,
  address string,
  ward int,
  community int,
  community_name string,
  tot_install int,
  x int,
  y int,
  historical int,
  f12 string,
  f13 string,
  location string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/chibike';
