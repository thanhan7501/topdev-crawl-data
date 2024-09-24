import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.SparkConf
import java.time.Instant

object sparkSetup {
  private lazy val connectionString = "mongodb://localhost:27017"
  private lazy val database = "topdev"
  private lazy val conf = new SparkConf()
    .setAppName("myApp")
    .setMaster("spark://127.0.0.1:7077")
    .set("spark.mongodb.read.connection.uri", connectionString)
    .set("spark.mongodb.write.connection.uri", connectionString)
    .set(
      "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.3.0"
    )
    .set(
      "spark.jars.packages", "org.postgresql:postgresql:42.7.3"
    )
    .set("spark.sql.debug.maxToStringFields", "1000")

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val mongodb: DataFrameReader = spark.read.format("mongodb").option("database", database)
}
