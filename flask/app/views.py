from app import app
from flask import render_template
from flask import request
from flask import jsonify
from redis import StrictRedis
from shapely.geometry import shape, Point
from shapely.strtree import STRtree
from cassandra.cluster import Cluster
import json

def RedisGeo(topic, longitude,latitude):
    redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')
    return redis_conn.georadius(topic, float(longitude), float(latitude), 1000, 'ft', True, True, False, None, 'ASC')
 
@app.route('/')
@app.route('/index')
def index():
  #lat = request.args.get('lat')
  #log = request.args.get('log')
  return render_template("index.html", title="NYC")

@app.route('/redis')
def get_gps():
    latitude = request.args.get('lat')
    longitude = request.args.get('long')
    response = RedisGeo("bus", longitude, latitude)
    jsonresponse = [{"type": "bus", "bus_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    response = RedisGeo("TaxiData", longitude, latitude)
    jsonresponse = jsonresponse + [{"type": "taxi", "taxi_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    return jsonify(jsonresponse)

@app.route('/cass')
def get_hist():
    latitude1 = float(request.args.get('lat1'))
    longitude1 = float(request.args.get('long1'))
    latitude2 = float(request.args.get('lat2'))
    longitude2 = float(request.args.get('long2'))
    with open('/tmp/neighborhoods.geojson', 'r') as f:
        js = json.load(f)
    pickup_point = Point(longitude1, latitude1)
    dropoff_point = Point(longitude2, latitude2)
    count = 0
    for feature in js['features']:
    	polygon = shape(feature['geometry'])
    	if polygon.contains(pickup_point) :
            pickup = feature["properties"]["neighborhood"]
	    count += 1
	if polygon.contains(dropoff_point) :
            dropoff = feature["properties"]["neighborhood"]
	    count += 1
	if count == 2:
	    break
    cluster = Cluster(['172.31.6.66'])
    session = cluster.connect('playground')
    rows = session.execute("SELECT * FROM test WHERE pickup = %s and dropoff = %s", (pickup, dropoff))	    
    jsonresponse = [{"type": "test", "duration": row.duration/row.sum, "cost": row.cost/row.sum, "count": row.sum } for row in rows]
    return jsonify(jsonresponse)
