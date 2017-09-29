# Kafka producer that reads the input bus data in a loop in order to simulate real time events
from kafka import KafkaProducer, KafkaConsumer
import time

source_file = '/tmp/bus_time_20170101.csv'

def genData(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True:
	with open(source_file) as f:
            count = 0
            for line in f:
		print(line)
                producer.send(topic, line.rstrip()) 
	        if (count % 100 ==0) :
                    time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map
        	count = count+1
        
	f.close()

genData("BusData")
