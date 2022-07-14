package sparktypesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object CommonTypesExercises extends App {

  /**
   * Exercises:
   *
   * Filter carsDF by a list of car names (eg. getting list of strings from an API call)
   */
  val spark = SparkSession.builder()
    .appName("Common Spark Types Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val carList: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")
  val carRegex: String = carList.map(_.toLowerCase()).mkString("|")

  val carsInListDF = carsDF
    .select(col("Name"), regexp_extract(col("Name"), carRegex, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop(col("regex_extract"))
    .show()

}
