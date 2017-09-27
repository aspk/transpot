import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import magellan.{Point, PolyLine, Polygon}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.magellan.dsl.expressions._
import com.redislabs.provider.redis._
import com.redis.RedisClient
import java.net.{ServerSocket, Socket}
import java.io._
import java.util.Calendar

import org.apache.commons.math3.ml.neuralnet.SquareNeighbourhood
import org.apache.spark.rdd.RDD
import org.gavaghan.geodesy._

object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TranSpot")
      .set("spark.cassandra.connection.host", "172.31.6.66")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "172.31.5.117:9092")

    val spark = SparkSessionSingleton.getInstance(sc.getConf)
    import spark.implicits._

    val neighborhoods = spark.sqlContext.read
      .format("magellan")
      .option("type", "geojson")
      .load("hdfs://172.31.4.94:9000/tmp/neighborhoods/")
      .select($"polygon", $"metadata" ("neighborhood").as("neighborhood"))

    val topic = ("TaxiData","BikeData")
    val topics1 = Set(topic._1)
    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics1)

    stream1.map { x => x._2.split(",") }.foreachRDD { rdd =>
      streamProcessing (rdd, topic._1, neighborhoods, sc, spark)
    }

    val topics2 = Set(topic._2)
    val stream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics2)

    stream2.map { x => x._2.split(",") }.foreachRDD { rdd =>
      streamProcessing (rdd, topic._2, neighborhoods, sc, spark)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def streamProcessing(rdd: RDD[Array[String]], topic: String, neighborhoods: DataFrame, sc:SparkContext, spark: SparkSession) : Unit = {
    import spark.implicits._
    val trans_type= topic.toLowerCase.replace("data","")

    val transactions = rdd.map { x =>
      ingestData(x, trans_type)
    }.filter(x => x match {
      case null => false
      case Record(pickup_longitude,pickup_latitude,pickup_date,pickup_time,dropoff_longitude,dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance) => true
    })

    transactions.saveToCassandra("playground", trans_type + "_history",
      SomeColumns("pickup_date","pickup_time","pickup_x","pickup_y","dropoff_date","dropoff_time", "dropoff_x", "dropoff_y", "cost","duration","distance"))

    val joined = transactions
      .toDF()
      .withColumn("pickup_point", point($"pickup_x", $"pickup_y"))
      .withColumn("dropoff_point", point($"dropoff_x", $"dropoff_y"))
      .join(neighborhoods).where($"pickup_point" within $"polygon")
      .withColumnRenamed("neighborhood", "pickup_neighborhood").drop("polygon")
      .join(neighborhoods).where($"dropoff_point" within $"polygon")
      .withColumnRenamed("neighborhood", "dropoff_neighborhood").drop("polygon")

    joined.rdd.foreachPartition { partition =>
      sendToRedis (trans_type, partition)
    }

    val curr = joined.rdd.map { x =>
      ((trans_type,x.getAs[String]("pickup_neighborhood"), x.getAs[String]("dropoff_neighborhood")),
        (1, x.getAs[Float]("cost").toDouble, x.getAs[Int]("duration").toLong))
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

    val prev = sc.cassandraTable[(String,String, String, Int, Double, Long)]("playground", "geoaggregate")
      .select("type","pickup","dropoff","sum","cost","duration")
      .map { x =>((x._1,x._2, x._3),(x._4,x._5,x._6))}

    curr.leftOuterJoin(prev).map{ x =>
      (x._1._1,x._1._2, x._1._3,
        x._2._1._1 + x._2._2.getOrElse(0,0.0,0L)._1,
        x._2._1._2 + x._2._2.getOrElse(0,0.0,0L)._2,
        x._2._1._3 + x._2._2.getOrElse(0,0.0,0L)._3)}
      .saveToCassandra("playground", "geoaggregate",
        SomeColumns("type" as "_1",
          "pickup" as "_2",
          "dropoff" as "_3",
          "sum" as "_4",
          "cost" as "_5",
          "duration" as "_6"))
  }

  def ingestData(row: Array[String], trans_type: String): Record = {
    if (trans_type == "taxi") {
      if (row.length <= 1 || (!isAllDigits(row(0)))) {
        return null
      } else {
        val pickup_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(row(1))
        val pickup_timestamp = new java.sql.Date(pickup_parsed.getTime())
        val dropoff_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(row(2))
        val dropoff_timestamp = new java.sql.Date(dropoff_parsed.getTime())
        val duration = (dropoff_timestamp.getTime() / 1000 - pickup_timestamp.getTime() / 1000).toInt
        val pickup_longitude = row(5).toDouble
        val pickup_latitude = row(6).toDouble
        val dropoff_longitude = row(7).toDouble
        val dropoff_latitude = row(8).toDouble
        val cost = row(18).toFloat
        val distance = row(10).toFloat
        val pickup_date = row(1).split("\\s+")(0)
        val pickup_time = row(1).split("\\s+")(1)
        val dropoff_date = row(2).split("\\s+")(0)
        val dropoff_time = row(2).split("\\s+")(1)

        return createRecord(trans_type, pickup_longitude,pickup_latitude,pickup_date,pickup_time,dropoff_longitude,dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance)
        }
    } else if (trans_type == "bike"){
      if (row.length <= 1 || (!isAllDigits(row(0)))) {
        return null
      } else {
        val pickup_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(row(1))
        val pickup_timestamp = new java.sql.Date(pickup_parsed.getTime())
        val dropoff_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(row(2))
        val dropoff_timestamp = new java.sql.Date(dropoff_parsed.getTime())
        val duration = row(0).toInt
        val duration_calc = (dropoff_timestamp.getTime() / 1000 - pickup_timestamp.getTime() / 1000).toInt
        val pickup_longitude = row(6).toDouble
        val pickup_latitude = row(5).toDouble
        val dropoff_longitude = row(10).toDouble
        val dropoff_latitude = row(9).toDouble
        val cost = 0
        val distance = 0
        val pickup_date = row(1).split("\\s+")(0)
        val pickup_time = row(1).split("\\s+")(1)
        val dropoff_date = row(2).split("\\s+")(0)
        val dropoff_time = row(2).split("\\s+")(1)

        if (math.abs(duration-duration_calc) <= 5 && duration >= 60) {
          return createRecord(trans_type, pickup_longitude,pickup_latitude,pickup_date,pickup_time,dropoff_longitude,dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance)
        } else {
          return null
        }
      }
    } else {
        return null
    }
  }

  def sendToRedis(trans_type: String, rows: Iterator[Row]): Unit = {
    val redis_conn = new RedisClient("172.31.3.244", 6379, 0, Some("3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8"))
    rows.foreach { x =>
      redis_conn.geoadd(trans_type, Seq((x.getAs[String]("dropoff_x"), x.getAs[String]("dropoff_y"), x.getAs[String]("dropoff_time"))))
    }
    redis_conn.disconnect
  }

  def createRecord(trans_type: String, pickup_longitude: Double, pickup_latitude: Double, pickup_date: String, pickup_time: String, dropoff_longitude: Double, dropoff_latitude: Double, dropoff_date: String, dropoff_time: String, cost: Float, duration: Int, distance: Float): Record = {
    val geoCalc = new GeodeticCalculator
    val reference = Ellipsoid.WGS84

    val pickup_point = new GlobalPosition(pickup_latitude, pickup_longitude, 0.0)
    val dropoff_point = new GlobalPosition(dropoff_latitude, dropoff_longitude, 0.0)

    val distance_calc = geoCalc.calculateGeodeticCurve(reference, pickup_point, dropoff_point).getEllipsoidalDistance // Distance between Point A and Point B

    if (trans_type == "taxi" && (duration <= 0 || cost <= 0  || distance * 1609.34 < distance_calc)){
      return null
    } else {
      return Record(pickup_longitude, pickup_latitude, pickup_date, pickup_time,
        dropoff_longitude, dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance)
    }
  }

  def isAllDigits(x: String) = (x != "") && (x forall Character.isDigit)
}

case class Record(pickup_x: Double, pickup_y: Double, pickup_date: String, pickup_time: String,
                  dropoff_x: Double, dropoff_y: Double, dropoff_date: String, dropoff_time: String,
                  cost: Float, duration: Int, distance: Float)

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
