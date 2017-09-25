from app import app
from flask import render_template
from flask import request
from flask import jsonify
from redis import StrictRedis

def RedisGeo(topic, longitude,latitude):
    redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')
    return redis_conn.georadius(topic, float(longitude), float(latitude), 1000, 'ft', True, True, False, None, 'ASC')
 
@app.route('/')
@app.route('/index')
def index():
  #lat = request.args.get('lat')
  #log = request.args.get('log')
  return render_template("index.html", title="NYC")

@app.route('/map/<latitude>/<longitude>')
def get_bus(latitude, longitude):
    response = RedisGeo(longitude, latitude)
    buslist=[]
    for x in response:
	buslist.append([x[2][0],x[2][1]])
    return render_template("show.html", title = 'Bus within 1000 ft', buslist = buslist, lat = latitude, long = longitude)

@app.route('/api')
def get_bus2():
    latitude = request.args.get('lat')
    longitude = request.args.get('long')
    response = RedisGeo("bus", longitude, latitude)
    jsonresponse = [{"bus_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    response = RedisGeo("TaxiData", longitude, latitude)
    jsonresponse = jsonresponse + [{"taxi_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    return jsonify(jsonresponse)

@app.route('/json/<latitude>/<longitude>')
def get_bus3(latitude, longitude):
    response = RedisGeo("bus", longitude, latitude)
    jsonresponse = [{"bus_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    response = RedisGeo("TaxiData", longitude, latitude)
    jsonresponse = jsonresponse + [{"taxi_id": x[0], "distance": x[1], "longitude": x[2][0], "latitude": x[2][1]} for x in response]
    return jsonify(jsonresponse)


@app.route('/test/')
def test():
    return render_template("test.html")

@app.route('/test/add/')
def add_numbers():
    """Add two numbers server side, ridiculous but well..."""
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)
    return jsonify(result = a + b)
