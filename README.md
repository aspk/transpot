# TranSpot - "Spotting all transportations nearby and optting for the right one"
Repository for my data engineering project done during Insight fellowship program

## External links for the demo

 * [Slides](http://www.bit.ly/transpot_slides)
 * [Video Cap of Live Demo](http://www.bit.ly/transpot_video)

 ## Index

1. [Introduction](README.md#1-introduction)
2. [The Pipeline](README.md#2-the-pipeline)
 * 2.1 [Data and Ingestion](README.md#21-ingestion)
 * 2.2 [Stream Processing](README.md#22-streaming)
 * 2.3 [WebUI](README.md#23-webui)
3. [Cluster configuration](README.md#3-cluster-configuration)
4. [References](README.md#4-references)

## 1. Introduction

Due to hideous traffic condition in the metro area, the time and cost for a taxi trip is highly unpredicable. To further advocate the utilization of the already avaialable bike share system, a side by side comparison of taxi and bike is desirable.

This project utilizes the public available taxi, bike and bus data from New York City to display the avilable transportations nearby and the historical trip average of the taxi and bike.

![Example](https://github.com/funfine/transpot/blob/master/demo.png)

## 2. The Pipeline


![Pipeline](https://github.com/funfine/transpot/blob/master/pipeline.png)


### 2.1 Data and Ingestion

 Three types of raw data has been ingesed through Kafka
  - NYC archived MTA transit data [data source](http://data.beta.nyc/dataset/unofficial-mta-transit-data-archive/resource/106dd52f-8755-40a0-aa3d-dfa6195a8d21)
  - NYC taxi trip record data [data source](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
  - NYC Citi Bike trip data [data source](https://s3.amazonaws.com/tripdata/index.html)

 The Kafka producer module can be accessed from [here](https://github.com/funfine/transpot/tree/master/DataIngestion)

 In the producer, the header for the CSV file is parsed, and column order was determined by keyword matching. Different date/time format was unified.

 ### 2.2 Stream processing

The Spark Streaming processing can be found [here](https://github.com/funfine/transpot/blob/master/StreamProcessing/src/main/scala/com/insightde2017/App.scala)

Two Kafka topics was directly consumed by Spark (Bus data only have gps information, and was consumed by a python script separately).

There are four steps in the streaming process
 - Data cleanning: filter the records to remove
    * zero coordinates/timestamp/cost
    * recorded distance shorter than cartesian distance
    * very short/long duration         
 - Send the GPS information to Redis: The data was written to a queue, and sent to Redis in batches to improve efficiency.
 - Geojoin the coordinates to the pre-determined neighborhoods and calculate the historial average between any two neighborhoods.
 - Save the raw trip data and the historial average to different table in Cassandra

###  2.3 WebUI

Google Map API was called to display the map. Tornado was applied to support the webserver. JQuery library was utilized to work with Flask to query the databases.

The front-end files can be found [here](https://github.com/funfine/transpot/tree/master/WebUI)

## 3 Cluster configuration
A total of 12 AWS EC2 t2.medium instances were used for this project, 3 of which is used for Kafka, 4 for Spark, 1 for Redis, 3 for Cassandra and last one for webserver. They were setup using [Pegasus](https://github.com/InsightDataScience/pegasus).

Kafka topics were created with 15 partitions and a replication factor of 2.
Spark cluster has 6 cores per worker, a total of 18 cores.
Cassandra cluster has a total capacity of 300GB, with a replication factor of 2.
Redis instance has a 4GB memory, with on disk persistence.

## 4 References

Kafka-spark connector can be found [here](https://spark.apache.org/docs/2.1.1/streaming-kafka-0-8-integration.html)
Redis-spark connector can be found [here](https://github.com/debasishg/scala-redis)
Cassandra-spark connector can be found [here](https://github.com/datastax/spark-cassandra-connector)
Redis driver in python can be found [here](https://github.com/andymccurdy/redis-py)
Cassandra driver in python can be found [here](https://github.com/datastax/python-driver)
Magellan API used for Geojoin with spark can be found [here](https://github.com/harsha2010/magellan)
