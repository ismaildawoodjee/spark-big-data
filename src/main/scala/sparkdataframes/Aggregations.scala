package sparkdataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // count  all values INCLUDING null
  val genresCount = movies.select(col("Major_Genre")).count()
  println(genresCount)
  movies.selectExpr("count(Major_Genre)").show()

  val genresCount2 = movies.select(count(col("Major_Genre")))
  genresCount2.show()

  // count all rows, will also include nulls
  val genresCount3 = movies.select(count("*"))
  genresCount3.show()

  // count distinct values within a column
  movies.select(countDistinct(col("Major_Genre"))).show()

  // approximate count for very large datasets, so that Spark doesn't run out of memory
  movies.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = movies.select(min(col("IMDB_Rating")))
  val maxRatingDF = movies.select(max(col("IMDB_Rating")))
  maxRatingDF.show()
  movies.selectExpr("min(IMDB_Rating)").show()

  // sum of a column
  println("Total US Gross Profit:")
  movies.select(sum(col("US_Gross"))).show()
  movies.selectExpr("sum(US_Gross)").show()

  // avg of a column
  movies.select(avg(col("Rotten_Tomatoes_Rating")))
  movies.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // statistics
  movies.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // group by and aggregations
  val countByGenreDF = movies
    .groupBy("Major_Genre")
    .count()
  countByGenreDF.show()

  // average IMDB rating by genre
  val avgRatingByGenreDF: Unit = movies
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")
    .show()

  // aggregations: perform an aggregation after a groupBy method
  val aggregationsByGenreDF: Unit = movies
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    .show()
}
