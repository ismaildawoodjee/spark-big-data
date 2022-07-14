package sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select IMDB or Rotten Tomatoes rating, whichever one is not null
  // `coalesce` function: takes the first cascading non-null value in a list of columns
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show()

  // checking for nulls
  moviesDF
    .select("*")
    .where(col("Rotten_Tomatoes_Rating").isNull)
    .show()

  // ordering nulls first or last
  moviesDF
    .orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last) // nulls come last
    .show()

  // removing rows containing null: use the methods from the .na method and its functions
  moviesDF
    .select("Title", "IMDB_Rating")
    .na.drop()
    .show()

  // replacing nulls with a constant value for multiple columns
  moviesDF
    .na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
    .show()

  // replacing nulls with different values for different columns
  moviesDF
    .na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  )).show()

  // more complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // does the same thing as `coalesce`
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // also does the same thing as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else will return first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2"
    // if the first argument is not null, it will return the second arg, else it will return the third
    // if (first != null) second else third
  ).show()
}
