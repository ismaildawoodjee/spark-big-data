package sparkdataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema: StructType = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /**
   * Reading a dataframe consists of the following:
   * - Format (json, csv, parquet)
   * - Schema (optional, can be inferred or can be specified manually)
   * - zero or more options:
   *   - mode: failFast | dropMalformed | permissive (default)
   *   - path: where is file? local or S3 or HDFS, etc. specify file path
   *
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // what to do when Spark encounters malformed data? specify with mode
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // Spark evaluation is lazy, so carsDF is only loaded when a show() method is called on it
  // errors will only show up after DF is loaded
  carsDF.show()

  // alternative reading method with options map. can pass the options Map as a dynamic variable
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/cars.json"
    ))
    .load()

  /**
   * writing DFs require the following:
   * - format
   * - save mode (what to do if file already exists?): overwrite | append | ignore | errorIfExists
   * - path
   * - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()
  /**
   * cars_dupe.json is going to be a folder containing files
   * part-uid is the actual json data
   * _SUCCESS is marker file for spark to validate completion of writing job
   * .crc files to validate integrity of other files
   */

}
