package sparklowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RDDExercises extends App {
  /**
   * Exercises:
   *
   * 1. Read the movies.json as an RDD.
   *
   * 2. Show the distinct genres as an RDD.
   *
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   *
   * 4. Show the average rating of movies by genre.
   *
   */
  val spark = SparkSession.builder()
    .appName("RDD Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Movie(title: String, genre: String, rating: Double)

  import spark.implicits._

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating"))
    .where(
      col("genre").isNotNull
        and col("rating").isNotNull)
    .as[Movie]
    .rdd
  moviesRDD.toDF().show()

  // 2. distinct genres
  val distinctGenresRDD = moviesRDD.map(_.genre).distinct()
  distinctGenresRDD.toDF().show()

  // 3.
  val goodDramaMoviesRDD = moviesRDD
    .filter(movie => movie.genre == "Drama" && movie.rating > 6)
  goodDramaMoviesRDD.toDF().show()

  // 4.
  case class GenreAvgRating(genre: String, avgRating: Double)

  val avgGenreRatingRDD = moviesDF
    .groupBy(col("Major_Genre").as("genre"))
    .agg(avg(col("IMDB_Rating")).as("avgRating"))
    .as[GenreAvgRating]
    .rdd
  avgGenreRatingRDD.toDF().show()
  // can also write
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()

  // alternatively,
  val avgRatingByGenreRDD = moviesRDD
    .groupBy(_.genre)
    .map {
      case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }
  avgGenreRatingRDD.toDF().show()
}
