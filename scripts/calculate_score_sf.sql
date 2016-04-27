DROP TABLE sf_agg;
CREATE EXTERNAL TABLE sf_agg (
  address string,
  location string,
  larceny int,
  robbery int,
  vehicle int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/sfscore';

DROP TABLE sf_rank;
CREATE TABLE sf_rank AS
SELECT address, location,
RANK() OVER (ORDER BY Larceny DESC) AS larc_rank,
RANK() OVER (ORDER BY Robbery DESC) AS robb_rank,
RANK() OVER (ORDER BY Vehicle DESC) AS veh_rank
FROM sf_agg;

DROP TABLE sf_avg_rank;
CREATE TABLE sf_avg_rank AS
SELECT address, location,
(larc_rank + robb_rank + veh_rank)/3 AS avg_rank
FROM sf_rank;

DROP TABLE sf_score;
CREATE TABLE sf_score AS
SELECT address, location,
AVG_RANK*10/(MAX(avg_rank) OVER ()) AS score
FROM sf_avg_rank;
