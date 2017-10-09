from kafka import KafkaProducer, KafkaConsumer
import time, sys
from utils_function import parse_header

# Kafka producer ingests both yellow and green taxi data
# The header of the csv file is parsed to determine the column index
# The column order of produced message is consistent across years


directory = sys.argv[1]


def gen_data(topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=str.encode, value_serializer=str.encode)
    for x in xrange(int(sys.argv[2]), int(sys.argv[3])):
        source_file = directory + str(x).zfill(2) + ".csv"
        count = 0
        with open(source_file) as f:
            column_index = {}
            for line in f:
                message_array = line.rstrip().split(',')
                if len(message_array) == 0:
                    continue
                if count == 0:
                    parse_header(message_array, column_index)
                    count += 1
                    continue
                message = ','.join((message_array[column_index['pickup_long']],
                                    message_array[column_index['pickup_lat']],
                                    message_array[column_index['pickup_datetime']],
                                    message_array[column_index['dropoff_long']],
                                    message_array[column_index['dropoff_lat']],
                                    message_array[column_index['dropoff_datetime']],
                                    message_array[column_index['cost']],
                                    message_array[column_index['distance']]))
                producer.send(topic, key=str(count), value=message)
                if count % 10000 == 0:
                    print (message)
#                    time.sleep(0.05)
                count += 1
        f.close()


gen_data("TaxiData")
