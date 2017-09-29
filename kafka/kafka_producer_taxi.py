# Kafka producer that reads the input taxi data in a loop in order to simulate real time events
from kafka import KafkaProducer, KafkaConsumer
import time

source_file1 = '/tmp/green_tripdata_2016-01.csv'
source_file2 = '/tmp/yellow_tripdata_2016-01.csv'

def genData(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=str.encode, value_serializer=str.encode)
    while True:
        produceData(topic, producer, source_file1)
        produceData(topic, producer, source_file2)
        
def produceData(topic, producer, source_file):
    with open(source_file) as f:
        count = 0
        for line in f:
            producer.send(topic, key = str(count), value = line.rstrip())
            print (line.rstrip())
            if (count % 100 == 0):
                time.sleep(0.1)
            count =count+1
    f.close()

genData("TaxiData")
