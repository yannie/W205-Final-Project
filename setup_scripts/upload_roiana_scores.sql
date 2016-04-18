DROP TABLE roiana_scores;
CREATE EXTERNAL TABLE roiana_scores (
  address string,
  location string,
  street_name string,
  racks int,
  spaces int,
  score double
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/roianascores';
