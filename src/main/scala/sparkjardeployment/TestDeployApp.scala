package sparkjardeployment

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {
  /**
   * Get good comedies (genre == comedy and IMDB_rating > 6.5).
   * The configuration for the Spark job will be given to the master node as
   * command line arguments. This Spark job is packaged as a jar and deployed
   * on the local Spark Docker containers as a test application.
   *
   * @param args : movies.json as arg(0) and goodComedies as arg(1) file paths
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Need input and output file paths.")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF
      .select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date"))
      .where(
        col("Major_Genre") === "Comedy"
          and col("IMDB_Rating") > 6.5)
      .orderBy(
        col("Rating").desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }
}
