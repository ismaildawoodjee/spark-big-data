package sparkdataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExercises extends App {
  /**
   * Exercises:
   *
   * 1. Sum up ALL the profits of ALL the movies in the movies DF
   *
   * 2. Count how many distinct directors we have
   *
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   *
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */
  val spark: SparkSession = SparkSession.builder()
    .appName("Aggregation Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // all the profits of all movies
  val profitsDF: sql.DataFrame = moviesDF.select(
    col("Title"),
    (col("US_Gross") + col("Worldwide_Gross")).as("TotalProfits")
  )
  profitsDF.selectExpr("sum(TotalProfits)").show()

  // alternatively
  val totalGrossDF = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross")).as("TotalGross"))
    .select(sum("TotalGross"))
    .show()

  // number of distinct directors
  val distinctDirectors: Unit = moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // mean and std of US gross
  val meanAndStdUSGross = moviesDF
    .select(
      mean(col("US_Gross")),
      stddev(col("US_Gross"))
    )
  meanAndStdUSGross.show()

  //
  val avgRatingAvgUSGrossPerDirector = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      avg("US_Gross").as("Avg_US_Revenue")
    )
    .orderBy(col("Avg_US_Revenue").desc_nulls_last)
    .show()

}
