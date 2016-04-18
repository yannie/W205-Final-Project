from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import os

engine = create_engine('hive://w205@54.173.20.8:10000/default')
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

class BikeStops2(Resource):
  def get(self):
    parser = reqparse.RequestParser()
    parser.add_argument('lat', type=float)
    parser.add_argument('long', type=float)
    args = parser.parse_args()
    lat = args['lat']
    long = args['long']
    print "lat: {0}, long: {1}".format(lat,long)

    conn = engine.connect()
    query = ("SELECT "
             "b.address, "
             "3956*2*ASIN(SQRT(POWER(SIN((b.x - abs({0})) * pi()/180/2), 2) "
             "+ COS(b.x*pi()/180)*COS(abs({0})*pi()/180) "
             "* POWER(SIN((b.y - {1})*pi()/180/2),2))) as distance "
             "FROM sf_bike_parking_base AS b "
             "WHERE "
             "b.x BETWEEN {0}-(1/69) AND {0}+(1/69) "
             "AND b.y BETWEEN {1}-(1/abs(cos({0}*pi()/180)*69)) AND {1}+(1/abs(cos({0}*pi()/180)*69)) "
             "ORDER BY distance "
             "LIMIT 10 ").format(lat, long)
    print query
    query_result = conn.execute(query)
    result = {"bike parking spot, distance": [(i[0], i[1]) for i in query_result.cursor.fetchall()]}
    print result
    conn.close()
    return result

class BikeStops3(Resource):
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
    distances = {}
    for i in query_result.cursor.fetchall():
      distances[i[0]] = i[1]
    query_scores = ("SELECT address, score FROM roiana_scores WHERE address IN (%s)" % distances.keys())
    print query_scores
    query_scores_result = conn.execute(query_scores)
    scores = {}
    for i in query_scores_result.cursor.fetchall():
      scores[i[0]] = i[1]
    result = {"address, distance, score": [(address, distances[address], scores[address]) for address in distances.keys()]}
    print result
    conn.close()
    return result

api.add_resource(Crimes, "/crimes/<int:num>")
api.add_resource(BikeStops, "/bikestops", endpoint='bikestops')
api.add_resource(BikeStops2, "/bikestops2", endpoint='bikestops2')
api.add_resource(BikeStops3, "/bikestops3", endpoint='bikestops3')

if __name__ == '__main__':
	test_con = engine.connect()
	test_query = "SELECT * FROM crime_table LIMIT 1"
	test_result = test_con.execute(test_query)
	print test_result.cursor.fetchall()
	port = int(os.environ.get("PORT", 8080))
	app.run(host='0.0.0.0', port=port, debug=True)
