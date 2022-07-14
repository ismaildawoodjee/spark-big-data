package sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF - adding the plain value "47" (any type)
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // filtering with Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  // multiple ways of filtering
  moviesDF.select("Title").where(preferredFilter).show()

  // filter on a Boolean column
  val goodMovies = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  goodMovies.where("good_movie").show()

  // negations
  goodMovies.where(not(col("good_movie")))

  /** filtering with numbers */
  // math operations (e.g. divide column by 10 and take average of two columns)
  val moviesAvgRatingsDF = moviesDF
    .select(col("Title"), ((col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2).as("avgRating"))
    .orderBy(col("avgRating").desc_nulls_last)
    .show()

  // correlation (Pearson corr coef lies between [-1,1]. Correlation is an ACTION, it is not evaluated lazily, like TRANSFORMATIONS
  val corrBetweenTwoRatings: Double = moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")
  println(s"Correlation between RT and IMDB Ratings is: $corrBetweenTwoRatings")
  println()

  /** String operations */
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap (capitalize first letter), lower, upper
  carsDF.select(initcap(col("name"))).show()

  // normal "contains" method works for one filter, but for multiple filters (e.g., volkswagen, vw, etc.), use regex
  carsDF.select("*").where(col("Name").contains("Volkswagen"))

  // regex filtering: empty string is returned when regex expression is not matched
  val volkswagenRegexFilter = "volkswagen|vw"
  val vwCarsDF = carsDF
    .select(col("Name"), regexp_extract(col("Name"), volkswagenRegexFilter, 0).as("vwRegex"))
    .where(col("vwRegex") =!= "")
    .drop("vwRegex")

  // replacement on a regex-matched string
  val vwReplacedCarsDF = vwCarsDF
    .select(col("Name"), regexp_replace(col("Name"), volkswagenRegexFilter, "German Car").as("vwReplaced"))
    .show()

}
