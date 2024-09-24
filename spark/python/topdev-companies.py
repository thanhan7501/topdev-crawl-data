import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameReader

from dotenv import load_dotenv

load_dotenv()

connectionString = os.getenv("MONGODB_URI")
postgresql_jdbc = os.getenv("POSTGRESQL_JDBC")
postgresql_user = os.getenv("POSTGRESQL_USER")
postgresql_password = os.getenv("POSTGRESQL_PASSWORD")
database = "topdev"
postgresql_conf = {
    "user": postgresql_user,
    "password": postgresql_password,
    "driver": "org.postgresql.Driver",
}

conf = (
    SparkConf()
    .setAppName("myApp")
    .set("spark.mongodb.read.connection.uri", connectionString)
    .set("spark.mongodb.write.connection.uri", connectionString)
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .set("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .set("spark.sql.debug.maxToStringFields", "1000")
)

spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
mongodbcf: DataFrameReader = spark.read.format("mongodb").option("database", database)

dfCompanies = mongodbcf.option("collection", "companies").load()
dfCompanies.createOrReplaceTempView("companies")

finalDfCompanies = spark.sql(
    """
    SELECT
      _id AS id
    , image_logo AS image
    , display_name AS name
    , company_size AS size
    , num_employees
    , industries_str AS industry
    , nationalities_str AS nationality
    , addresses.full_addresses[0] AS address
    , detail_url AS url
    , inserted_at
    , updated_at
    FROM companies
  """
)

finalDfCompanies.show()

finalDfCompanies.write.mode("append").jdbc(
    f"{postgresql_jdbc}/topdev", "companies", properties=postgresql_conf
)
