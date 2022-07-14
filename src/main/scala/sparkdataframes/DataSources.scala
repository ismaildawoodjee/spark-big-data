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
    //    StructField("Year", DateType),
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

  /**
   * Reading JSON
   * - if JSON has dates, can pass in "dateformat" option - Spark will parse it
   * - allowSingleQuotes: Bool = treat single quotes the same way as double quotes
   * - compression: uncompressed (default) | bzip2 | gzip | lz4 | snappy | deflate
   * -
   */
  spark.read
    .schema(carsSchema)
    //    .option("dateformat", "YYYY-MM-dd") // only works with enforced schema - if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")
    .json("src/main/resources/data/cars.json")

  /**
   * Reading CSV
   * - header: Bool = if CSV has a header to specify first row header or not
   * - sep (separator): , (default), \t, | (etc.)
   */
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    //    StructField("date", DateType),
    StructField("date", StringType),
    StructField("price", DoubleType),
  ))
  val stocksDF = spark.read
    .schema(stocksSchema)
    //    .option("dateformat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.show()

  /**
   * Parquet: open-source compressed binary data storage format (optimized for columnar data)
   * Can specify .parquet("path/name.parquet")
   * or can specify .save(path/name.parquet) since parquet is default data format for Spark
   * Same folder structure as the cars.json folder will be created, but with 6x compression factor
   */
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  /**
   * Text files: every line in a single column will be considered a value
   */
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from a remote database (like Postgres, MySQL)
  // run the Postgres docker container first (adjusted port is 5433)
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver") // specifically a DB driver for postgres
    .option("url", "jdbc:postgresql://localhost:5433/rtjvm") // DB URI
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees") // table you want to read
    .load()

  employeesDF.show()
}
