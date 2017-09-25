# Kafka producer that reads the input bus data in a loop in order to simulate real time events
import os
import sys
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import time

source_file = '/tmp/green_tripdata_2016-01.csv'

def genData(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=str.encode, value_serializer=str.encode)
    while True:
        with open(source_file) as f:
            count = 0
            for line in f:
                producer.send(topic, key = str(count), value = line.rstrip())
		print (line.rstrip())
	        time.sleep(0.05)  # Creating some delay to allow proper rendering of the cab locations on the map
        	count =count+1
        source_file.close()

genData("TaxiData")
