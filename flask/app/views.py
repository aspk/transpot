from app import app
from flask import render_template, request, jsonify
from redis import StrictRedis
from shapely.geometry import shape, Point
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import json, time, os


# This file is to query the redis and cassandra database,
# return the results to front end

with open("configurations.json", 'r') as f:
    SETTINGS = json.load(f)


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html", title="NYC transportation")


# This function is to query the redis database
# and get the vehicle nearby based on the position of user click
@app.route('/redis')
def get_gps():
    latitude = request.args.get('lat')
    longitude = request.args.get('long')

    # Retrieve the current timestamp and compare to the timestamp of the vehicle
    # Only pass it to the frontend when the vehicle is within a pre-specified time window
    os.environ["TZ"] = "US/Pacific"
    time.tzset()
    cur = time.strftime("%X", time.localtime())

    topics = ["bus", "bike", "taxi"]
    jsonresponse = []

    for topic in topics:
        response = RedisGeo(topic, longitude, latitude)
        if len(response) == 0:
            continue
        for row in response:
            vehicle_id = row[0].split(" ")[0]
            past = row[0].split(" ")[1]
            time_format = '%H:%M:%S'
            time_delta = datetime.strptime(cur, time_format) - datetime.strptime(past, time_format)
            if time_delta.seconds < SETTINGS['TIMEWINDOW']:
                jsonresponse += [{"type": topic, "vehicleid": vehicle_id,
                                  "time": past, "distance": row[1],
                                  "longitude": row[2][0], "latitude": row[2][1]}]
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

    if response == "":
        return ""

    jsonresponse = [{"type": row.type,
                     "duration": row.duration / row.count,
                     "distance": row.distance / row.count,
                     "cost": row.cost / row.count,
                     "count": row.count}
                    for row in response]
    return jsonify(jsonresponse)


# helper function to connect to Redis
def redis_geo(topic, longitude, latitude):
    redis_server = SETTINGS['REDIS_IP']
    redis_password = SETTINGS['REDIS_PASSWORD']
    redis_conn = StrictRedis(host=redis_server, port=6379, db=0, password=redis_password)
    return redis_conn.georadius(topic, float(longitude), float(latitude), SETTINGS['DISTANCE'], 'ft', True, True, False, None, 'ASC')


def cassandra_history(pickup, dropoff):
    cluster = Cluster([SETTINGS['CASSANDRA_IP']])
    session = cluster.connect('transpot')
    if pickup == "" or dropoff == "":
        return ""
    return session.execute("SELECT * FROM geoaggregate WHERE pickup = %s and dropoff = %s", (pickup, dropoff))


def geojoin(longitude1, latitude1, longitude2, latitude2):
    with open('/tmp/neighborhoods.geojson', 'r') as f:
        js = json.load(f)
    pickup_point = Point(longitude1, latitude1)
    dropoff_point = Point(longitude2, latitude2)
    count = 0
    pickup = ""
    dropoff = ""
    for feature in js['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(pickup_point):
            pickup = feature["properties"]["neighborhood"]
            count += 1
        if polygon.contains(dropoff_point):
            dropoff = feature["properties"]["neighborhood"]
            count += 1
        if count == 2:
            break
    return pickup, dropoff
