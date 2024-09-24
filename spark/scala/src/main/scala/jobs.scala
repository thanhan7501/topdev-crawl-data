import sparkSetup.{mongodb, spark}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, explode}
import org.bson.Document
import postgresql.connectionProperties

object jobs {
  def loadJobsData(): Unit = {
    val dfJobs = mongodb.option("collection", "jobs").load()
    val explodedDf = dfJobs.withColumn("requirement", explode(col("requirements_arr")))
    val joinedDf = explodedDf.withColumn("joined_values", concat_ws(", ", col("requirement.value")))
    val combinedDf = joinedDf.groupBy("_id").agg(concat_ws(", ", collect_list(col("joined_values"))).as("combined_values"))
    dfJobs.createOrReplaceTempView("jobs")
    combinedDf.createOrReplaceTempView("requirements")

    val finalDfJobs = spark.sql(
      """
        |SELECT
        |  j._id AS id
        |, title
        |, content
        |, owned_id
        |, skills_str AS skills
        |, salary.value AS salary
        |, detail_url AS url
        |, combined_values AS requirement
        |, inserted_at
        |, updated_at
        |FROM jobs j
        |INNER JOIN requirements r
        |ON j._id = r._id
      """.stripMargin
    )

    finalDfJobs.show()

    finalDfJobs.write.mode("overwrite").jdbc("jdbc:postgresql://localhost:5432/topdev", "jobs", connectionProperties)
  }
}
