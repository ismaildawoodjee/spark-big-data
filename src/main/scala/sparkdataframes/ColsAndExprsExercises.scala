package sparkdataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ColsAndExprsExercises extends App {
  /**
   * Exercises
   *
   * 1. Read the movies dataframe and select 2 columns of your choice
   * 2. Create another column, summing up total gross profit (US, Worldwide, DVD sales)
   * 3. Select all Comedy genre movies, but only the good ones (with IMDB rating > 6)
   */
  val spark = SparkSession.builder()
    .appName("Columns and Expressions Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val twoColsMovies = movies.selectExpr("Title", "Worldwide_Gross")
  twoColsMovies.show()

//  val moviesProfitDF = movies.select(
//    col("Title"),
//    col("US_Gross"),
//    col("Worldwide_Gross"),
//    col("US_DVD_Sales"),
//    (col("US_Gross") + col("Worldwide_Gross")).as("TotalProfit")
//  )
//  moviesProfitDF.show()

  // summing with nulls will make the total null as well (nulls must be treated separately)
  val moviesWithGrossProfitCol = movies.withColumn(
    "TotalGrossProfit",
    col("US_Gross") + col("Worldwide_Gross")
  )
  moviesWithGrossProfitCol.show()

  val goodComedies = movies.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  goodComedies.show()
}
