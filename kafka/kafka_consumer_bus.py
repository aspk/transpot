# This program implements a kafka consumer that stores a data stream to Redis. It is subscribed to one topic (all bus data). 
from kafka import KafkaProducer, KafkaConsumer
from redis import StrictRedis
	
def consume_topic(topic, group):
    print "Consumer Loading topic '%s' in consumer group %s" % (topic, group)
    consumer = KafkaConsumer(topic, group_id=group, bootstrap_servers=['localhost:9092'])
    count = 1
    list = []
    for message in consumer:
	tmp = message.value.split(',')
	if (tmp[0] == 'timestamp'):
	    continue

	timestamp = tmp[0].split('T')[1][:-1]
	vehicle_id = tmp[1]
	lat = tmp[2]
	log = tmp[3]	
	list.extend([float(log), float (lat), str(vehicle_id) + " " +str(timestamp)])
	print (timestamp)
	if (count % 1000 == 0):
	    redis_conn.geoadd(group, *list)
	    list = []
	count = count + 1
	print (count)


if __name__ == '__main__':
    group = "bus"
    topic = "BusData"
    redis_conn = StrictRedis(host='172.31.3.244', port=6379, db=0, password = '3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8')


    print "\nConsuming topic: [%s] into Redis" % topic
    consume_topic(topic, group)
                                                     
