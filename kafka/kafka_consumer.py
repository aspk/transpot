# This program implements a kafka consumer that stores a data stream to Redis. It is subscribed to one topic (all cab data). 
import os
import sys
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
from redis import StrictRedis
	
# Function for storing data from the producer into a temporary file (which is later stored to HDFS through another function call) 
def consume_topic(topic, group):
    print "Consumer Loading topic '%s' in consumer group %s" % (topic, group)
    consumer = KafkaConsumer(topic, group_id=group, bootstrap_servers=['localhost:9092'])
    for message in consumer:
	print (message)
	tmp = message.value.split(',')
	timestamp = tmp[0]
	vehicle_id = tmp[1]
	lat = tmp[2]
	log = tmp[3]
	print "timestamp: %s" %(timestamp)
	print "vehicle_id: %s" %(vehicle_id)
	print "latitude: %s" %(lat)
	print "longitude: %s" %(log)
	if (timestamp != 'timestamp'):
	    redis_conn.geoadd(group, float(lat), float(log), vehicle_id)


if __name__ == '__main__':
    group = "bus"
    topic = "BusData"
    redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')


    print "\nConsuming topic: [%s] into Redis" % topic
    consume_topic(topic, group)
                                                     
