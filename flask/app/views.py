from app import app
from flask import render_template
from flask import request
from flask import jsonify
from redis import StrictRedis
from shapely.geometry import shape, Point
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import json
import time
import os
 
# This file is to query the redis and cassandra database, 
# return the results to front end

@app.route('/')
@app.route('/index')
def index():
  return render_template("index.html", title="NYC transportations")

# This function is to query the redis database 
# and get the vehicle nearby based on the position of user click
@app.route('/redis')
def get_gps():
    latitude = request.args.get('lat')
    longitude = request.args.get('long')

    # Retrieve the current timestamp and compare to the timestamp of the vehicle
    # Only pass it to the frontend when the vehicle is within 10 minutes window 
    os.environ["TZ"]="US/Pacific"
    time.tzset()
    cur = time.strftime("%X", time.localtime())
    
    topics = ["bus","bike","taxi"]
    jsonresponse = []
    
    for topic in topics:
    	response = RedisGeo(topic, longitude, latitude)
	for row in response:
            vehicle_id = row[0].split(" ")[0] 
	    past = row[0].split(" ")[1]
	    FMT = '%H:%M:%S'
	    tdelta = datetime.strptime(cur, FMT) - datetime.strptime(past, FMT)
	    if (tdelta.seconds < 7200):
                jsonresponse = jsonresponse + [{"type": topic, 
                                                "vehicleid": vehicle_id, 
                                                "time": past, "distance": row[1], 
                                                "longitude": row[2][0], 
                                                "latitude": row[2][1]}]
    return jsonify(jsonresponse)

# This function is to query the cassandra database and get the historic average 
@app.route('/cass')
def get_hist():
    latitude1 = float(request.args.get('lat1'))
    longitude1 = float(request.args.get('long1'))
    latitude2 = float(request.args.get('lat2'))
    longitude2 = float(request.args.get('long2'))
    
    # Geo-join to determine the neighborhoods of start and end point
    neighborhood = GeoJoin(longitude1, latitude1, longitude2, latitude2)

    # Query the cassandra to get the historical average
    response = CassandraHistory(neighborhood[0], neighborhood[1])

    jsonresponse = [{"type": row.type, 
                     "duration": row.duration/row.count, 
                     "distance": row.distance/row.count, 
                     "cost": row.cost/row.count, 
                     "count": row.count } 
                    for row in response]
    return jsonify(jsonresponse)

# helper function to connect to Redis
def RedisGeo(topic, longitude,latitude):
    redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')
    return redis_conn.georadius(topic, float(longitude), float(latitude), 1000, 'ft', True, True, False, None, 'ASC')

def CassandraHistory(pickup, dropoff):
    cluster = Cluster(['172.31.6.66'])
    session = cluster.connect('transpot')
    return session.execute("SELECT * FROM geoaggregate WHERE pickup = %s and dropoff = %s", (pickup, dropoff))	    

def GeoJoin(longitude1, latitude1, longitude2, latitude2):
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
    return (pickup, dropoff)