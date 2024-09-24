import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_list, concat_ws, explode, from_json, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
)

kafkaBroker = os.getenv("BROKER")
postgresql_jdbc = os.getenv("POSTGRESQL_JDBC")
postgresql_user = os.getenv("POSTGRESQL_USER")
postgresql_password = os.getenv("POSTGRESQL_PASSWORD")
kafka_bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")

postgresql_conf = {
    "user": postgresql_user,
    "password": postgresql_password,
    "driver": "org.postgresql.Driver",
}

schemaJob = StructType(
    [
        StructField("_id", IntegerType()),
        StructField("applied", StringType()),
        StructField("title", StringType()),
        StructField("content", StringType()),
        StructField("detail_url", StringType()),
        StructField(
            "inserted_at",
            StructType(
                [
                    StructField("$date", LongType()),
                ]
            ),
        ),
        StructField(
            "updated_at",
            StructType(
                [
                    StructField("$date", LongType()),
                ]
            ),
        ),
        StructField("owned_id", IntegerType()),
        StructField("skills_str", IntegerType()),
        StructField(
            "requirements_arr",
            ArrayType(
                StructType(
                    [
                        StructField("value", ArrayType(StringType())),
                        StructField("icon", StringType()),
                    ]
                )
            ),
        ),
        StructField(
            "salary",
            StructType(
                [
                    StructField("is_negotiable", StringType()),
                    StructField("unit", StringType()),
                    StructField("min", StringType()),
                    StructField("max", StringType()),
                    StructField("currency", StringType()),
                    StructField("min_estimate", StringType()),
                    StructField("max_estimate", StringType()),
                    StructField("currency_estimate", StringType()),
                    StructField("value", StringType()),
                ]
            ),
        ),
        StructField(
            "benefits",
            ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType()),
                        StructField("name", StringType()),
                        StructField("value", StringType()),
                    ]
                )
            ),
        ),
    ]
)

def write_stream(batch_df: DataFrame, batch_id: int):
    value_df = batch_df.select(col("value").cast("string").alias("value"))
    transform_value_df = value_df.select(
        from_json(col("value"), schemaJob).alias("document")
    )
    exploded_df = transform_value_df.select(col("document.*"))
    exploded_df.show(truncate=False)

    exploded_req_df = exploded_df.withColumn(
        "requirement", explode(col("requirements_arr"))
    )
    joined_req__df = exploded_req_df.withColumn(
        "joined_req_values", concat_ws(", ", col("requirement.value"))
    )
    combined_req_df = joined_req__df.groupBy("_id").agg(
        concat_ws(", ", collect_list(col("joined_req_values"))).alias(
            "combined_req_values"
        )
    )

    exploded_benefits_df = exploded_df.withColumn("benefit", explode(col("benefits")))
    joined_benefits_df = exploded_benefits_df.withColumn(
        "joined_benefits_values", concat_ws(", ", col("benefit.value"))
    )
    combined_benefits_df = joined_benefits_df.groupBy("_id").agg(
        concat_ws(", ", collect_list(col("joined_benefits_values"))).alias(
            "combined_benefits_values"
        )
    )

    final_df_jobs = (
        exploded_df.join(combined_req_df, "_id", "leftouter")
        .join(combined_benefits_df, "_id", "leftouter")
        .select(
            col("_id").alias("id"),
            col("title"),
            col("content"),
            col("owned_id"),
            col("skills_str").alias("skills"),
            col("detail_url").alias("url"),
            col("combined_req_values").alias("requirement"),
            col("combined_benefits_values").alias("benefit"),
            (col("inserted_at.$date") / 1000).cast("timestamp").alias("inserted_at"),
            (col("updated_at.$date") / 1000).cast("timestamp").alias("updated_at"),
            when(col("salary.is_negotiable").cast("int") == 1, "Thương lượng")
            .otherwise(col("salary.value"))
            .alias("salary"),
        )
    )

    final_df_jobs.show(truncate=False)

    final_df_jobs.write.mode("append").jdbc(
        f"{postgresql_jdbc}/topdev", "jobs", properties=postgresql_conf
    )


conf = (
    SparkConf()
    .setAppName("myStreamingApp")
    .set(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    .set("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .set("spark.sql.debug.maxToStringFields", "1000")
)
spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

df_jobs = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_server)
    .option("subscribe", "topdev.jobs")
    .option("startingOffsets", "earliest")
    .load()
)

df_jobs.writeStream.foreachBatch(write_stream).option(
    "checkpointLocation", "chk-point-dir"
).start().awaitTermination()
