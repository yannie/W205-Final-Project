from flask import Flask, request
from flask_restful import Resource, Api, reqparse
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import os

engine = create_engine('hive://w205@52.23.193.92:10000/default')
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

api.add_resource(Crimes, "/crimes/<int:num>")
api.add_resource(BikeStops, "/bikestops", endpoint='bikestops')

if __name__ == '__main__':
	test_con = engine.connect()
	test_query = "SELECT * FROM crime_table LIMIT 1"
	test_result = test_con.execute(test_query)
	print test_result.cursor.fetchall()
	port = int(os.environ.get("PORT", 8080))
	app.run(host='0.0.0.0', port=port, debug=True)
