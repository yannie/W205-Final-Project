DROP TABLE c_test;
CREATE EXTERNAL TABLE c_test (
  Address string,
  Location string,
  Larceny int,
  Robbery int,
  Vehicle int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/finscore';

DROP TABLE C_GROUP;
CREATE TABLE C_GROUP AS
SELECT Address, Location,
AVG(Larceny) AS LARC,
AVG(Robbery) AS ROB,
AVG(Vehicle) as VEH
FROM c_test
GROUP BY ADDRESS, LOCATION;

DROP TABLE C_RANK;
CREATE TABLE C_RANK AS
SELECT Address, Location,
RANK() OVER (ORDER BY LARC ASC) AS LARC_RANK,
RANK() OVER (ORDER BY ROB ASC) AS ROBB_RANK,
RANK() OVER (ORDER BY VEH ASC) AS VEH_RANK
FROM C_GROUP;

DROP TABLE C_AVG;
CREATE TABLE C_AVG AS
SELECT Address, Location,
(LARC_RANK + ROBB_RANK + VEH_RANK)/3 AS AVG_RANK
FROM C_RANK;

DROP TABLE C_SCORE;
CREATE TABLE C_SCORE AS
SELECT Address, Location,
AVG_RANK*10/(MAX(AVG_RANK) OVER ()) AS SCORE
FROM C_AVG;