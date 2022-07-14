package sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // how to parse dates, if they are already read as strings?
  val moviesWithDate = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithDate
    .withColumn("Today", current_date()) // returns today's date
    .withColumn("Right_Now", current_timestamp()) // returns this second's timestamp
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
  // .show() // deprecated datetime format
  // there are also other date functions, like `date_add` and `date_sub`

  /**
   * Structures: structs are groups of columns aggregated into one (kind of like a tuple of columns)
   */
  // e.g. combine US_Gross and Worldwide_Gross into one column
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    .show()

  // struct within a selectExpr
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
    .show()

  // ARRAY of strings (splitting a string with several delimiters)
  val moviesWithWords = moviesDF
    .select(col("Title"), split(col("Title"), pattern = " |,").as("Title_Words"))

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
