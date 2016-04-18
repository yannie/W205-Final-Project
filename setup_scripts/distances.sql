DROP TABLE bike_parking_distances;
CREATE TABLE bike_parking_distances AS
SELECT
  address,
  X-(1/69) as min_x,
  X+(1/69) as max_x,
  Y-(1/abs(cos(X*pi()/180)*69)) as min_y,
  Y+(1/abs(cos(X*pi()/180)*69)) as max_y
FROM sf_bike_parking_base;
