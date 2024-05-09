import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, AnalysisException}
import java.io.FileNotFoundException


object ProviderReport {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("ProviderReport")
      .master("local[*]")
      .getOrCreate()

    try {
      // Read providers data with proper delimiter
      val providersSchema = StructType(Array(
        StructField("provider_id", IntegerType, nullable = false),
        StructField("provider_specialty", StringType, nullable = false),
        StructField("first_name", StringType, nullable = false),
        StructField("middle_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = false)
      ))
      val providersDF = spark.read
        .schema(providersSchema)
        .option("header", "true")
        .option("delimiter", "|")
        .csv("data/providers.csv")

      // Read visits data
      val visitsSchema = StructType(Array(
        StructField("visit_id", IntegerType, nullable = false),
        StructField("provider_id", IntegerType, nullable = false),
        StructField("date_of_service", DateType, nullable = false)
      ))
      val visitsDF = spark.read
        .schema(visitsSchema)
        .option("header", "true")
        .csv("data/visits.csv")

      // Problem 1: Calculate total number of visits per provider
      val totalVisitsPerProvider = visitsDF.groupBy("provider_id")
        .agg(count("*").alias("total_visits"))

      // Join with providers data to get provider's name and specialty
      val report1 = totalVisitsPerProvider.join(providersDF, Seq("provider_id"))
        .select("provider_id", "first_name", "last_name", "provider_specialty", "total_visits")

      // Output the report partitioned by specialty
      report1.write.partitionBy("provider_specialty").json("output/total_visits_per_provider")

      // Problem 2: Calculate total number of visits per provider per month
      val visitsPerProviderPerMonth = visitsDF.withColumn("month", date_format(col("date_of_service"), "yyyy-MM"))
        .groupBy("provider_id", "month")
        .agg(count("*").alias("total_visits"))

      // Output the report partitioned by month
      visitsPerProviderPerMonth.write.partitionBy("month").json("output/visits_per_provider_per_month")
    } catch {
      case e: FileNotFoundException =>
        println("Error: File not found. Make sure the file exists at the specified path.")
      case e: AnalysisException =>
        println("Error: An analysis error occurred while processing the data.")
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
