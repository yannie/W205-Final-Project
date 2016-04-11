DROP TABLE crime_table;
CREATE EXTERNAL TABLE crime_table (
  IncidntNum int,
  Category string,
  Descript string,
  DayOfWeek string,
  Date date,
  Time int,
  PdDistrict string,
  Resolution string,
  Address string,
  X int,
  Y int,
  Location string,
  PdId int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/sfcrime'
tblproperties ("skip.header.line.count"="1");

DROP TABLE sf_bike_parking_base;
CREATE EXTERNAL TABLE sf_bike_parking_base (
  address string,
  location_name string,
  street_name string,
  racks int,
  spaces int,
  placement string,
  mo_installed int,
  yr_installed int,
  coordinates string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/sfbike'
tblproperties ("skip.header.line.count"="1");


DROP TABLE COUNT_CRIME;
CREATE TABLE COUNT_CRIME AS
SELECT CATEGORY, COUNT(CATEGORY) AS CT_CAT FROM CRIME_TABLE
GROUP BY CATEGORY
ORDER BY CT_CAT DESC;

SELECT * FROM COUNT_CRIME;


DROP TABLE COUNT_DES_CRIME;
CREATE TABLE COUNT_DES_CRIME AS
SELECT DESCRIPT, COUNT(DESCRIPT) AS CT_DESCR FROM CRIME_TABLE
GROUP BY DESCRIPT
ORDER BY CT_DESCR DESC;

SELECT * FROM COUNT_DES_CRIME;

DROP TABLE BIKE_DATE;
CREATE TABLE BIKE_DATE AS
SELECT DATE FROM CRIME_TABLE WHERE DESCRIPT = 'GRAND THEFT BICYCLE'
;

SELECT * FROM BIKE_DATE;

DROP TABLE PET_BIKE_DATE;
CREATE TABLE PET_BIKE_DATE AS
SELECT DATE FROM CRIME_TABLE WHERE DESCRIPT = 'PETTY THEFT BICYCLE';

SELECT * FROM PET_BIKE_DATE;


DROP TABLE BIKE_THEFT;
CREATE TABLE BIKE_THEFT AS
SELECT * FROM CRIME_TABLE
WHERE DESCRIPT = 'GRAND THEFT BICYCLE' OR DESCRIPT = 'PETTY THEFT BICYCLE';

DROP TABLE BIKE_LOC;
CREATE TABLE BIKE_LOC AS
SELECT PDDISTRICT, COUNT(PDDISTRICT) AS CT_PDDIST FROM BIKE_THEFT
GROUP BY PDDISTRICT
ORDER BY CT_PDDIST DESC;

DROP TABLE BIKE_DATE;
CREATE TABLE BIKE_DATE AS
SELECT DATE, COUNT(DATE) AS CT_DATE FROM BIKE_THEFT
GROUP BY DATE
ORDER BY CT_DATE DESC;


DROP TABLE BIKE_DATE;
CREATE TABLE BIKE_DATE AS
SELECT DATE, COUNT(DATE) AS CT_DATE FROM BIKE_THEFT
GROUP BY DATE
ORDER BY DATE;

DROP TABLE BIKE_DAY;
CREATE TABLE BIKE_DAY AS
SELECT DAYOFWEEK, COUNT(DAYOFWEEK) AS CT_DAY FROM BIKE_THEFT
GROUP BY DAYOFWEEK
ORDER BY CT_DAY;
