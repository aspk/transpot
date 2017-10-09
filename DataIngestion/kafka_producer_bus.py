from kafka import KafkaProducer, KafkaConsumer
import time, sys


# Kafka producer that reads the input bus data in a loop in order to simulate real time events


source_file = sys.argv[1]


def gen_data(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True:
        with open(source_file) as f:
            count = 0
            for line in f:
                print(line)
                producer.send(topic, line.rstrip())
                if count % 100 == 0:
                    time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map
                count = count + 1

        f.close()


gen_data("BusData")
