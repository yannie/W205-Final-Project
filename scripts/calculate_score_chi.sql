DROP TABLE chi_agg;
CREATE EXTERNAL TABLE chi_agg (
  address string,
  rack_id int,
  larceny int,
  robbery int,
  vehicle int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/chiscore';

DROP TABLE chi_rank;
CREATE TABLE chi_rank AS
SELECT address, rack_id,
RANK() OVER (ORDER BY larceny DESC) AS larc_rank,
RANK() OVER (ORDER BY robbery DESC) AS robb_rank,
RANK() OVER (ORDER BY vehicle DESC) AS veh_rank
FROM chi_agg;

DROP TABLE chi_avg;
CREATE TABLE chi_avg AS
SELECT address, rack_id,
(larc_rank + robb_rank + veh_rank)/3 AS avg_rank
FROM chi_rank;

DROP TABLE chi_score;
CREATE TABLE chi_score AS
SELECT address, rack_id,
AVG_RANK*10/(MAX(avg_rank) OVER ()) AS score
FROM chi_avg;
