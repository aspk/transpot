from flask import Flask
from flask_googlemaps import GoogleMaps
app = Flask(__name__)
GoogleMaps(app)
from app import views
