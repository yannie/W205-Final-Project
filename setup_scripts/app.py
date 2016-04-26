from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import os
import re

# Update this to point to public ip address of ec2 instance.
engine = create_engine('hive://w205@54.174.226.237:10000/default')
app = Flask(__name__)
api = Api(app)

class Crimes(Resource):
  def get(self, num):
    conn = engine.connect()
    query = conn.execute("SELECT incidntnum FROM crime_table LIMIT %d" % num)
    print query
    result = {"incident number": [i[0] for i in query.cursor.fetchall()]}
    print result
    conn.close()
    return result

class BikeStops(Resource):
  def get(self):
    parser = reqparse.RequestParser()
    parser.add_argument('lat', type=float)
    parser.add_argument('long', type=float)
    args = parser.parse_args()
    lat = args['lat']
    long = args['long']
    print "lat: {0}, long: {1}".format(lat,long)

    conn = engine.connect()
    query = ("SELECT * FROM (SELECT address, "
             "3956*2*ASIN(SQRT(POWER(SIN(({0} - abs(x)) * pi()/180/2), 2) "
             "+ COS({0}*pi()/180) * COS(abs(x)*pi()/180) "
             "* POWER(SIN(({1} - y)*pi()/180/2),2))) AS distance "
             "FROM sf_bike_parking_base ) distances "
             "WHERE distance <= 1 "
             "ORDER BY distance "
             "LIMIT 5").format(lat, long)
    print query
    query_result = conn.execute(query)
    result = {"bike parking spot, distance": [(i[0], i[1]) for i in query_result.cursor.fetchall()]}
    print result
    conn.close()
    return result

def RemoveNonAsciiChar(text):
  return str(re.sub(r'[^\x00-\x7F]',r'', text))

class SFBikeStops(Resource):
  def get(self):
    parser = reqparse.RequestParser()
    parser.add_argument('lat', type=float)
    parser.add_argument('long', type=float)
    args = parser.parse_args()
    lat = args['lat']
    long = args['long']
    print "lat: {0}, long: {1}".format(lat,long)

    conn = engine.connect()
    query = ("SELECT * FROM "
	     "  (SELECT address, location_name, "
             "     3956*2*ASIN(SQRT(POWER(SIN(({0} - abs(x)) * pi()/180/2), 2) "
             "       + COS({0}*pi()/180) * COS(abs(x)*pi()/180) "
             "       * POWER(SIN(({1} - y)*pi()/180/2),2))) AS distance "
             "   FROM sf_bike_parking_base ) distances "
             "WHERE distance <= 0.25 ").format(lat, long)
    print query
    query_result = conn.execute(query)
    distances = {}
    addresses = []
    locations = []
    for i in query_result.cursor.fetchall():
      addresses.append(RemoveNonAsciiChar(i[0]))
      locations.append(RemoveNonAsciiChar(i[1]))
      distances[",".join((RemoveNonAsciiChar(i[0]), RemoveNonAsciiChar(i[1])))] = float(i[2])
    # Return early if no nearby bike racks found.
    if not distances:
      return "No nearby bike racks found."
    # Look up scores of nearby bike racks.
    query_scores = ("SELECT address, location, score FROM sf_score WHERE address IN ({0}) AND location IN ({1})").format(str(addresses).strip('[]'), str(locations).strip('[]'))
    print query_scores
    query_scores_result = conn.execute(query_scores)
    scores = {}
    for i in query_scores_result.cursor.fetchall():
      scores[",".join((RemoveNonAsciiChar(i[0]), RemoveNonAsciiChar(i[1])))] = float(i[2])
    # Sort locations by score.
    sorted_scores = sorted(scores.items(), key=lambda x:x[1], reverse=True)
    results = []
    max_range = min(5, len(sorted_scores))
    # Get top 5 locations ordered by score desc.
    for i in range(max_range):
      address_loc = sorted_scores[i][0]
      address_loc_tup = address_loc.split(",")
      address = address_loc_tup[0]
      location = address_loc_tup[1]
      score = sorted_scores[i][1]
      distance = distances[address_loc]
      results.append(("Address: {0}".format(address),
                      "Location name: {0}".format(location),
                      "Distance: {0:.2f}".format(distance),
                      "Score: {0:.2f}".format(score)))
    result = {"Recommended spots": results}
    print result
    conn.close()
    return result

class ChiBikeStops(Resource):
  def get(self):
    parser = reqparse.RequestParser()
    parser.add_argument('lat', type=float)
    parser.add_argument('long', type=float)
    args = parser.parse_args()
    lat = args['lat']
    long = args['long']
    print "lat: {0}, long: {1}".format(lat,long)

    conn = engine.connect()
    query = ("SELECT * FROM "
             "  (SELECT address, rack_id, "
             "     3956*2*ASIN(SQRT(POWER(SIN(({0} - abs(x)) * pi()/180/2), 2) "
             "       + COS({0}*pi()/180) * COS(abs(x)*pi()/180) "
             "       * POWER(SIN(({1} - y)*pi()/180/2),2))) AS distance "
             "   FROM chi_bike_parking_base ) distances "
             "WHERE distance <= 0.25 ").format(lat, long)
    print query
    query_result = conn.execute(query)
    distances = {}
    addresses = {}
    for i in query_result.cursor.fetchall():
      rack_id = int(i[1])
      distances[rack_id] = float(i[2])
      addresses[rack_id] = RemoveNonAsciiChar(i[0])
    # Return early if no nearby bike racks found.
    if not addresses:
      return "No nearby bike racks found."
    # Look up scores of nearby bike racks.
    query_scores = ("SELECT address, rack_id, score FROM chi_score WHERE rack_id IN ({0})").format(str(addresses.keys()).strip('[]'))
    print query_scores
    query_scores_result = conn.execute(query_scores)
    scores = {}
    for i in query_scores_result.cursor.fetchall():
      scores[int(i[1])] = float(i[2])
    # Sort locations by score.
    sorted_scores = sorted(scores.items(), key=lambda x:x[1], reverse=True)
    results = []
    max_range = min(5, len(sorted_scores))
    # Get top 5 locations ordered by score desc.
    for i in range(max_range):
      rack_id = sorted_scores[i][0]
      address = addresses[rack_id]
      score = sorted_scores[i][1]
      distance = distances[rack_id]
      results.append(("Address: {0}".format(address),
                      "Rack id: {0}".format(rack_id),
                      "Distance: {0:.2f}".format(distance),
                      "Score: {0:.2f}".format(score)))
    result = {"Recommended spots": results}
    print result
    conn.close()
    return result

api.add_resource(Crimes, "/crimes/<int:num>")
api.add_resource(BikeStops, "/bikestops", endpoint='bikestops')
api.add_resource(SFBikeStops, "/sf", endpoint='sf')
api.add_resource(ChiBikeStops, "/chi", endpoint='chi')

if __name__ == '__main__':
	test_con = engine.connect()
	test_query = "SELECT * FROM crime_table LIMIT 1"
	test_result = test_con.execute(test_query)
	print test_result.cursor.fetchall()
	port = int(os.environ.get("PORT", 8080))
	app.run(host='0.0.0.0', port=port, debug=True)
