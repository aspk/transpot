from kafka import KafkaProducer, KafkaConsumer
from redis import StrictRedis
import json


# This program implements a kafka consumer that stores a data stream to Redis.
# It is subscribed to one topic (all bus data).


with open("configurations.json", 'r') as f:
    SETTINGS = json.load(f)


def consume_topic(topic, group):
    print "Consumer Loading topic '%s' in consumer group %s" % (topic, group)
    consumer = KafkaConsumer(topic, group_id=group, bootstrap_servers=['localhost:9092'])
    count = 1
    geoadd_list = []
    for message in consumer:
        tmp = message.value.split(',')
        if tmp[0] == 'timestamp':
            continue

        timestamp = tmp[0].split('T')[1][:-1]
        vehicle_id = tmp[1]
        lat = tmp[2]
        log = tmp[3]
        geoadd_list.extend([float(log), float(lat), str(vehicle_id) + " " + str(timestamp)])
        print (timestamp)
        if count % 1000 == 0:
            redis_conn.geoadd(group, *geoadd_list)
            geoadd_list = []
        count = count + 1
        print (count)


if __name__ == '__main__':
    group = "bus"
    topic = "BusData"
    redis_server = SETTINGS['REDIS_IP']
    redis_password = SETTINGS['REDIS_PASSWORD']
    redis_conn = StrictRedis(host=redis_server, port=6379, db=0, password=redis_password)

    print "\nConsuming topic: [%s] into Redis" % topic
    consume_topic(topic, group)
