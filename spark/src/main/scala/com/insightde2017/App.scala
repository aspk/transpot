import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import magellan.{Point, PolyLine, Polygon}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.magellan.dsl.expressions._
import com.redis.RedisClient
import org.apache.spark.rdd.RDD
import org.gavaghan.geodesy._

object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TranSpot")
      .set("spark.worker.cleanup.enabled", "True")
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
    val keyspace = "transpot"

    // Ingest the cleaned data and save each trip to cassandra as the data warehouse
    // GPS information was striped off to send to redis
    val transactions = rdd.map { x =>
      ingestData(x, trans_type)
    }.filter(x => x match {
      case null => false
      case Record(id, pickup_longitude,pickup_latitude,pickup_date,pickup_time,dropoff_longitude,dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance) => true
    })

    transactions.saveToCassandra(keyspace, trans_type + "_history",
      SomeColumns("id","pickup_date","pickup_time","pickup_x","pickup_y","dropoff_date","dropoff_time", "dropoff_x", "dropoff_y", "cost","duration","distance"))

    transactions.foreachPartition { partition =>
      sendToRedis(trans_type, partition)
    }

    // geoaggregate the geo-points to the neighborhoods by magellan API
    val joined = transactions
      .toDF()
      .withColumn("pickup_point", point($"pickup_x", $"pickup_y"))
      .withColumn("dropoff_point", point($"dropoff_x", $"dropoff_y"))
      .join(neighborhoods).where($"pickup_point" within $"polygon")
      .withColumnRenamed("neighborhood", "pickup_neighborhood").drop("polygon")
      .join(neighborhoods).where($"dropoff_point" within $"polygon")
      .withColumnRenamed("neighborhood", "dropoff_neighborhood").drop("polygon")

    val curr = joined.rdd.map { x =>
      ((trans_type,x.getAs[String]("pickup_neighborhood"), x.getAs[String]("dropoff_neighborhood")),
        (1, x.getAs[Float]("cost").toDouble, x.getAs[Int]("duration").toLong, x.getAs[Float]("distance").toDouble))
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))

    val prev = sc.cassandraTable[(String,String, String, Int, Double, Long, Double)](keyspace, "geoaggregate")
      .select("type","pickup","dropoff","count","cost","duration", "distance")
      .map { x =>((x._1,x._2, x._3),(x._4,x._5,x._6, x._7))}

    curr.leftOuterJoin(prev).map{ x =>
      (x._1._1,x._1._2, x._1._3,
        x._2._1._1 + x._2._2.getOrElse(0,0.0,0L,0.0)._1,
        x._2._1._2 + x._2._2.getOrElse(0,0.0,0L,0.0)._2,
        x._2._1._3 + x._2._2.getOrElse(0,0.0,0L,0.0)._3,
        x._2._1._4 + x._2._2.getOrElse(0,0.0,0L,0.0)._4)}
      .saveToCassandra(keyspace, "geoaggregate",
        SomeColumns("type" as "_1",
          "pickup" as "_2",
          "dropoff" as "_3",
          "count" as "_4",
          "cost" as "_5",
          "duration" as "_6",
          "distance" as "_7"))
  }

  def ingestData(row: Array[String], trans_type: String): Record = {

    if (row.length <= 1 || (!isAllDigits(row(0)))) { // remove header and empty rows
      return null
    } else {
      if (trans_type == "taxi") { //parse for taxi data
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

        // data cleaning logic is inside the createTaxiRecord function
        // It will filter out the records when
        //    1. Calculated duration is too small or too large (usually indication of wrong timestamp)
        //    2. Cost is less than or equal to 0
        //    3. The distance is smaller than cartesian distance between pickup and dropoff point.
        return createTaxiRecord(pickup_longitude, pickup_latitude, pickup_date, pickup_time, dropoff_longitude, dropoff_latitude, dropoff_date, dropoff_time, cost, distance)

      } else if (trans_type == "bike") { // parse for bike data
        val duration = row(0).toInt
        val pickup_longitude = row(6).toDouble
        val pickup_latitude = row(5).toDouble
        val dropoff_longitude = row(10).toDouble
        val dropoff_latitude = row(9).toDouble
        val pickup_date = row(1).split("\\s+")(0)
        val pickup_time = row(1).split("\\s+")(1)
        val dropoff_date = row(2).split("\\s+")(0)
        val dropoff_time = row(2).split("\\s+")(1)
        val id = row(11).toInt

        // data cleaning logic is inside the createBikeRecord function
        // It will filter out the records when
        //    1. Calculated duration is more than 5s different from the recorded duration
        //    2. Duration is too small (less than 60s)
        return createBikeRecord(id, pickup_longitude, pickup_latitude, pickup_date, pickup_time, dropoff_longitude, dropoff_latitude, dropoff_date, dropoff_time, duration)

      } else { // other topics will not be parsed
        return null
      }
    }
  }

  def sendToRedis(trans_type: String, records: Iterator[Record]): Unit = {
    var redis_seq = Seq[(Double, Double, String)]()
    records.foreach{ x =>
      redis_seq = redis_seq :+ (x.dropoff_x, x.dropoff_y, x.id + " " + x.dropoff_time)
    }
    if (!redis_seq.isEmpty) {
      val redis_conn = new RedisClient("172.31.3.244", 6379, 0, Some("3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8"))
      redis_conn.geoadd(trans_type,redis_seq)
      redis_conn.disconnect
    }
  }

  def createTaxiRecord(pickup_longitude: Double, pickup_latitude: Double, pickup_date: String, pickup_time: String, dropoff_longitude: Double, dropoff_latitude: Double, dropoff_date: String, dropoff_time: String, cost: Float, distance: Float): Record = {
    val duration = calculateDuration(pickup_date, pickup_time, dropoff_date, dropoff_time)

    val geoCalc = new GeodeticCalculator
    val reference = Ellipsoid.WGS84

    val pickup_point = new GlobalPosition(pickup_latitude, pickup_longitude, 0.0)
    val dropoff_point = new GlobalPosition(dropoff_latitude, dropoff_longitude, 0.0)

    val distance_calc = geoCalc.calculateGeodeticCurve(reference, pickup_point, dropoff_point).getEllipsoidalDistance // Distance between Point A and Point B
    val id = 0

    if (duration <= 0 || duration > 2592000 || cost <= 0  || distance * 1609.34 < distance_calc){
      return null
    } else {
      return Record(id, pickup_longitude, pickup_latitude, pickup_date, pickup_time,
        dropoff_longitude, dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance)
    }
  }

  def createBikeRecord(id:Int, pickup_longitude: Double, pickup_latitude: Double, pickup_date: String, pickup_time: String, dropoff_longitude: Double, dropoff_latitude: Double, dropoff_date: String, dropoff_time: String, duration: Int): Record = {
    val duration_calc = calculateDuration(pickup_date, pickup_time, dropoff_date, dropoff_time)

    val cost = 0
    val distance = 0

    if (math.abs(duration-duration_calc) <= 5 && duration >= 60) {
      return Record(id, pickup_longitude, pickup_latitude, pickup_date, pickup_time, dropoff_longitude, dropoff_latitude, dropoff_date, dropoff_time, cost, duration, distance)
    } else {
      return null
    }
  }

  def calculateDuration (pickup_date: String, pickup_time: String, dropoff_date: String, dropoff_time: String): Int = {
    val pickup_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(pickup_date + " " + pickup_time)
    val pickup_timestamp = new java.sql.Date(pickup_parsed.getTime())
    val dropoff_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(dropoff_date + " " + dropoff_time)
    val dropoff_timestamp = new java.sql.Date(dropoff_parsed.getTime())

    return (dropoff_timestamp.getTime() / 1000 - pickup_timestamp.getTime() / 1000).toInt
  }

  def isAllDigits(x: String) = (x != "") && (x forall Character.isDigit)
}

case class Record(id: Int, pickup_x: Double, pickup_y: Double, pickup_date: String, pickup_time: String,
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
