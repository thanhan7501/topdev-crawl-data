import postgresql.connectionProperties
import sparkSetup.{mongodb, spark}
import org.apache.spark.sql.functions.col

object companies {
  def loadCompaniesData(): Unit = {
    val dfCompanies = mongodb.option("collection", "companies").load()
    dfCompanies.createOrReplaceTempView("companies")
    val finalDfCompanies = spark.sql(
      """
        |SELECT
        |  _id AS id
        |, image_logo AS image
        |, display_name AS name
        |, company_size AS size
        |, num_employees
        |, industries_str AS industry
        |, nationalities_str AS nationality
        |, addresses.full_addresses[0] AS address
        |, detail_url AS url
        |, inserted_at
        |, updated_at
        |FROM companies
      """.stripMargin
    )

    finalDfCompanies.show()

    finalDfCompanies.write.mode("overwrite").jdbc("jdbc:postgresql://localhost:5432/topdev", "companies", connectionProperties)
  }
}
