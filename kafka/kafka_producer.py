# Kafka producer that reads the input bus data in a loop in order to simulate real time events
import os
import sys
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import time
import urllib
import lzma
source_file = '/tmp/bus_time_20170101.csv'

def genData(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True:
	with open(source_file) as f:
            	for line in f:
			print(line)
                	producer.send(topic, line.rstrip()) 
	        	time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map
        
	source_file.close()

genData("BusData")
