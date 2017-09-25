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
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.magellan.dsl.expressions._
import com.redislabs.provider.redis._
import com.redis.RedisClient
import java.net.{ServerSocket, Socket}
import java.io._
import java.util.Calendar

object App {
  def main(args: Array[String]) {
    val topic = "TaxiData"
    val conf = new SparkConf().setAppName("TranSpot")
      .set("spark.cassandra.connection.host", "172.31.6.66")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "172.31.4.94:9092")

    val topics = Set(topic)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    val spark = SparkSessionSingleton.getInstance(sc.getConf)
    import spark.implicits._

    val neighborhoods = spark.sqlContext.read
      .format("magellan")
      .option("type", "geojson")
      .load("hdfs://172.31.4.94:9000/tmp/neighborhoods/")
      .select($"polygon", $"metadata" ("neighborhood").as("neighborhood"))


    stream.map { x => x._2.split(",") }.foreachRDD { rdd =>
      val trans = rdd.map { x =>
        val pickup_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(x(1))
        val pickup_time = new java.sql.Date(pickup_parsed.getTime())
        val dropoff_parsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(x(2))
        val dropoff_time = new java.sql.Date(dropoff_parsed.getTime())
        val duration = dropoff_time.getTime() / 1000 - pickup_time.getTime() / 1000

        Record(x(5).toDouble, x(6).toDouble, x(1).split(" ")(0), x(1).split("\\s+")(1),
          x(7).toDouble, x(8).toDouble, x(2).split("\\s+")(0), x(2).split(" ")(1), x(18).toDouble, duration)
      }
        .toDF()
        .withColumn("Pickup_Point", point($"Pickup_X", $"Pickup_Y"))
        .withColumn("Dropoff_Point", point($"Dropoff_X", $"Dropoff_Y"))
        .join(neighborhoods).where($"Pickup_Point" within $"polygon")
        .withColumnRenamed("neighborhood", "Pickup_neighborhood").drop("polygon")
        .join(neighborhoods).where($"Dropoff_Point" within $"polygon")
        .withColumnRenamed("neighborhood", "Dropoff_neighborhood").drop("polygon")

      trans.rdd.foreachPartition { partition =>
        val redis_conn = new RedisClient("172.31.3.244", 6379, 0, Some("3c5bac3ecc34b94cf2ecb65d0b7a2ba7d664221f0116c50ba5557a41804e83e8"))

        partition.foreach { x =>
          redis_conn.geoadd("test1", Seq((x.getAs[String]("Dropoff_X"), x.getAs[String]("Dropoff_Y"), x.getAs[String]("Dropoff_Time"))))
        }
        redis_conn.disconnect
      }

      val curr = trans.rdd.map { x =>
        ((x.getAs[String]("Pickup_neighborhood"), x.getAs[String]("Dropoff_neighborhood")),
          (1, x.getAs[Double]("Cost"), x.getAs[Long]("Duration")))
      }
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

      val prev = sc.cassandraTable[(String, String, Int, Double, Long)]("playground", "test")
                    .select("pickup","dropoff","sum","cost","duration")
                       .map { x =>
                         ((x._1,x._2),(x._3,x._4,x._5))
                       }

      curr.leftOuterJoin(prev).map{ x =>
        (x._1._1, x._1._2,
          x._2._1._1 + x._2._2.getOrElse(0,0.0,0L)._1,
          x._2._1._2 + x._2._2.getOrElse(0,0.0,0L)._2,
          x._2._1._3 + x._2._2.getOrElse(0,0.0,0L)._3)}
        .saveToCassandra("playground", "test",
                          SomeColumns("pickup" as "_1",
                                      "dropoff" as "_2",
                                      "sum" as "_3",
                                      "cost" as "_4",
                                      "duration" as "_5"))
    }

    ssc.start()
    ssc.awaitTermination()

  }
}

case class Record(Pickup_X: Double, Pickup_Y: Double, Pickup_Date: String, Pickup_Time: String,
                  Dropoff_X: Double, Dropoff_Y: Double, Dropoff_Date: String, Dropoff_Time: String,
                  Cost: Double, Duration: Long)

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
