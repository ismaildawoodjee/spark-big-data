package sparkdataframes

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourcesExercises extends App {
  /**
   * Exercise: read the movies DF, then write it as
   * - TSV file
   * - snappy parquet
   * - table "public.movies" in the Postgres DB
   */
  val spark = SparkSession.builder()
    .appName("Data Sources Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  // writing to TSV is like writing to CSV, but with tab separator
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("delimiter", "\t")
    .csv("src/main/resources/data/movies_dupe.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy")
    .save("src/main/resources/data/movies_dupe.parquet")

  // option specifications (e.g. URI, driver) could be saved as reusable variables as a good practice
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5433/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
