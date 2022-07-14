package sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object ComplexTypesExercises extends App {

  /**
   * Exercise:
   *
   * 1. How do we deal with multiple date formats? Parse the DF multiple times, then union the small DFs.
   *
   * 2. Read the stocks DF and parse the dates.
   *
   */
  val spark = SparkSession.builder()
    .appName("Complex Data Types Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithDatesDF = stocksDF
    .withColumn("symbol", to_date(col("date"), "MMM dd yyyy").as("formattedDate"))
  // .show()

  //  stocksDF.show()
}
