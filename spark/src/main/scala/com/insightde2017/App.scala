import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import magellan.{Point, PolyLine, Polygon}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.magellan.dsl.expressions._
import com.redis.RedisClient
import org.apache.spark.rdd.RDD
import org.gavaghan.geodesy._
import java.text.SimpleDateFormat
import scala.io.Source
import play.api.libs.json._

/* This app directly consumes from Kafka producers.
   The ingested data were first cleaned based on some reasonable rules.
   The trips were saved to the Cassandra database for future use.
   Then it geo-joins and aggregates the trip start and end points as the neighborhoods.
     The new york city was divided into ~300 neighborhoods area.
   Finally, the historical average was computed and the result was saved to Cassandra.
*/

object App {

  val relativePath = "configurations.json"
  val conf = getClass.getResource(relativePath)
  val json = Json.parse(Source.fromURL(conf).mkString)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TranSpot")
      .set("spark.worker.cleanup.enabled", "true")
      .set("spark.streaming.unpersist","true")
      .set("spark.streaming.concurrentJobs","2")
      .set("spark.driver.memory", "4g")
      .set("spark.cassandra.connection.host", json("CASSANDRA_IP").as[String])
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> json("KAFKA_IP").as[String])

    val spark = SparkSessionSingleton.getInstance(sc.getConf)
    import spark.implicits._

    val neighborhoods = spark.sqlContext.read
      .format("magellan")
      .option("type", "geojson")
      .load(json("NEIGHBORHOODS_FILE").as[String])
      .select($"polygon", $"metadata" ("neighborhood").as("neighborhood"))

    val topic = ("TaxiData","BikeData")

    val topics1 = Set(topic._1)
    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics1)
    val taxiIndex = new Index(-1,1,2,3,4,5,6,7,-1,8)

    stream1.foreachRDD { rdd =>
      streamProcessing (rdd, topic._1, neighborhoods, sc, spark, taxiIndex)
    }

    val topics2 = Set(topic._2)
    val stream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics2)
    val bikeIndex = new Index(7,1,2,3,4,5,6,-1,8,-1)

    stream2.foreachRDD { rdd =>
      streamProcessing (rdd, topic._2, neighborhoods, sc, spark, bikeIndex)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def streamProcessing(rdd: RDD[(String,String)], topic: String, neighborhoods: DataFrame, sc:SparkContext, spark: SparkSession, index: Index) : Unit = {
    import spark.implicits._
    val transType= topic.toLowerCase.replace("data","")
    val keySpace = "transpot"

    if (rdd.isEmpty()) {
      return
    }

    // Ingest the cleaned data and save each trip to cassandra as the data warehouse
    // GPS information was sent to redis
    val transactions = rdd.map { x =>
      ingestData(x, transType, index)
    }.filter(x => x match {
      case null => false
      case Record(tripId, vehicleId, pickupLongitude,pickupLatitude,pickupDate,pickupTime,dropoffLongitude,dropoffLatitude, dropoffDate, dropoffTime, cost, duration, distance) => true
    })

    transactions.saveToCassandra(keySpace, transType + "_history",
      SomeColumns("trip_id","pickup_date","pickup_time","pickup_x","pickup_y","dropoff_date","dropoff_time", "dropoff_x", "dropoff_y", "cost","duration","distance"))

    transactions.foreachPartition { partition =>
      sendToRedis(transType, partition)
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
      ((transType,x.getAs[String]("pickup_neighborhood"), x.getAs[String]("dropoff_neighborhood")),
        (1, x.getAs[Float]("cost").toDouble, x.getAs[Int]("duration").toLong, x.getAs[Float]("distance").toDouble))
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))

    val prev = sc.cassandraTable[(String,String, String, Int, Double, Long, Double)](keySpace, "geoaggregate")
      .select("type","pickup","dropoff","count","cost","duration", "distance")
      .map { x =>((x._1,x._2, x._3),(x._4,x._5,x._6, x._7))}

    curr.leftOuterJoin(prev).map{ x =>
      (x._1._1,x._1._2, x._1._3,
        x._2._1._1 + x._2._2.getOrElse(0,0.0,0L,0.0)._1,
        x._2._1._2 + x._2._2.getOrElse(0,0.0,0L,0.0)._2,
        x._2._1._3 + x._2._2.getOrElse(0,0.0,0L,0.0)._3,
        x._2._1._4 + x._2._2.getOrElse(0,0.0,0L,0.0)._4)}
      .saveToCassandra(keySpace, "geoaggregate",
        SomeColumns("type" as "_1",
          "pickup" as "_2",
          "dropoff" as "_3",
          "count" as "_4",
          "cost" as "_5",
          "duration" as "_6",
          "distance" as "_7"))
  }

  def ingestData(message: (String,String), transType: String, index: Index): Record = {
    val row = Array(message._1) ++ message._2.split(',')

    if (transType == "taxi") { //parse for taxi data
      val tripId = row(0).toLong
      val pickupLongitude = row(index.pickupLong).toDouble
      val pickupLatitude = row(index.pickupLat).toDouble
      val dropoffLongitude = row(index.dropoffLong).toDouble
      val dropoffLatitude = row(index.dropoffLat).toDouble
      val cost = row(index.cost).toFloat
      val distance = row(index.distance).toFloat
      val pickupDate = row(index.pickupDatetime).split("\\s+")(0)
      val pickupTime = row(index.pickupDatetime).split("\\s+")(1)
      val dropoffDate = row(index.dropoffDatetime).split("\\s+")(0)
      val dropoffTime = row(index.dropoffDatetime).split("\\s+")(1)

      // data cleaning logic is inside the createTaxiRecord function
      // It will filter out the records when
      //    1. Calculated duration is too small or too large (usually indication of wrong timestamp)
      //    2. Cost is less than or equal to 0
      //    3. The distance is smaller than cartesian distance between pickup and dropoff point.
      return createTaxiRecord(tripId, pickupLongitude, pickupLatitude, pickupDate, pickupTime, dropoffLongitude, dropoffLatitude, dropoffDate, dropoffTime, cost, distance)

    } else if (transType == "bike") { // parse for bike data
      val tripId = row(0).toLong
      val duration = row(index.duration).toInt
      val pickupLongitude = row(index.pickupLong).toDouble
      val pickupLatitude = row(index.pickupLat).toDouble
      val dropoffLongitude = row(index.dropoffLong).toDouble
      val dropoffLatitude = row(index.dropoffLat).toDouble
      val pickupDate = convertDate(row(index.pickupDatetime).split("\\s+")(0))
      val pickupTime = row(index.pickupDatetime).split("\\s+")(1)
      val dropoffDate = convertDate(row(index.dropoffDatetime).split("\\s+")(0))
      val dropoffTime = row(index.dropoffDatetime).split("\\s+")(1)
      val bikeId = row(index.id).toInt

      // data cleaning logic is inside the createBikeRecord function
      // It will filter out the records when
      //    1. Calculated duration is more than 5s different from the recorded duration
      //    2. Duration is too small (less than 60s)
      return createBikeRecord(tripId, bikeId, pickupLongitude, pickupLatitude, pickupDate, pickupTime, dropoffLongitude, dropoffLatitude, dropoffDate, dropoffTime, duration)

    } else { // other topics will not be parsed
      return null
    }
  }

  def sendToRedis(transType: String, records: Iterator[Record]): Unit = {
    var redisSeq = Seq[(Double, Double, String)]()
    records.foreach{ x =>
      redisSeq = redisSeq :+ (x.dropoff_x, x.dropoff_y, x.vehicle_id + " " + x.dropoff_time)
    }
    if (!redisSeq.isEmpty) {
      val redisConn = new RedisClient(json("REDIS_IP").as[String], 6379, 0, Some(json("REDIS_PASSWORD").as[String]))
      redisConn.geoadd(transType,redisSeq)
      redisConn.disconnect
    }
  }

  def createTaxiRecord(tripId: Long, pickupLongitude: Double, pickupLatitude: Double, pickupDate: String, pickupTime: String, dropoffLongitude: Double, dropoffLatitude: Double, dropoffDate: String, dropoffTime: String, cost: Float, distance: Float): Record = {
    val duration = calculateDuration(pickupDate, pickupTime, dropoffDate, dropoffTime)

    val geoCalc = new GeodeticCalculator
    val reference = Ellipsoid.WGS84

    val pickupPoint = new GlobalPosition(pickupLatitude, pickupLongitude, 0.0)
    val dropoffPoint = new GlobalPosition(dropoffLatitude, dropoffLongitude, 0.0)

    val distanceCalc = geoCalc.calculateGeodeticCurve(reference, pickupPoint, dropoffPoint).getEllipsoidalDistance // Distance between Point A and Point B
    val taxiId = 0

    if (duration <= 0 || duration > 2592000 || cost <= 0  || distance * 1609.34 < distanceCalc){
      return null
    } else {
      return Record(tripId, taxiId, pickupLongitude, pickupLatitude, pickupDate, pickupTime,
        dropoffLongitude, dropoffLatitude, dropoffDate, dropoffTime, cost, duration, distance)
    }
  }

  def createBikeRecord(tripId:Long, bikeId:Int, pickupLongitude: Double, pickupLatitude: Double, pickupDate: String, pickupTime: String, dropoffLongitude: Double, dropoffLatitude: Double, dropoffDate: String, dropoffTime: String, duration: Int): Record = {
    val duration_calc = calculateDuration(pickupDate, pickupTime, dropoffDate, dropoffTime)

    val cost = 0
    val distance = 0

    println("duration:" + duration)
    println("pickupDate:" + pickupDate)
    println("pickupTime:" + pickupTime)
    println("dropoffDate:" + dropoffDate)
    println("dropoffTime:" + dropoffTime)
    println("duration_calc:" + duration_calc)

    if (math.abs(duration-duration_calc) <= 5 && duration >= 60) {
      return Record(tripId, bikeId, pickupLongitude, pickupLatitude, pickupDate, pickupTime,
        dropoffLongitude, dropoffLatitude, dropoffDate, dropoffTime, cost, duration, distance)
    } else {
      return null
    }
  }

  def calculateDuration (pickupDate: String, pickupTime: String, dropoffDate: String, dropoffTime: String): Int = {
    val pickupParsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(pickupDate + " " + pickupTime)
    val pickupTimestamp = new java.sql.Date(pickupParsed.getTime())
    val dropoffParsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(dropoffDate + " " + dropoffTime)
    val dropoffTimestamp = new java.sql.Date(dropoffParsed.getTime())

    return (dropoffTimestamp.getTime() / 1000 - pickupTimestamp.getTime() / 1000).toInt
  }

  def convertDate(str: String): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("mm/dd/yyyy")
    val date = simpleDateFormat.parse(str)
    val df = new SimpleDateFormat("yyyy-mm-dd")
    return df.format(date)
  }

  def isAllDigits(x: String) = (x != "") && (x forall Character.isDigit)
}

case class Record(trip_id:Long, vehicle_id: Int, pickup_x: Double, pickup_y: Double, pickup_date: String,
                  pickup_time: String, dropoff_x: Double, dropoff_y: Double, dropoff_date: String,
                  dropoff_time: String, cost: Float, duration: Int, distance: Float)

@SerialVersionUID(15L)
class Index(var id: Int = -1, var pickupLong: Int = -1, var pickupLat: Int = -1, var pickupDatetime: Int = -1,
            var dropoffLong: Int = -1, var dropoffLat: Int = -1, var dropoffDatetime: Int = -1,
            var cost: Int = -1, var duration: Int = -1, var distance: Int = -1) extends Serializable

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
