package sparkdataframes

import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {
  // need a Spark session to run an application on top of Spark
  val spark: SparkSession = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local") // Spark Master Node is running on a local container
    .getOrCreate()

  // reading a DF
  val firstDF: sql.DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing/printing out the first N rows of the DF
  firstDF.show()

  // printing the schema of every column in the dataframe
  firstDF.printSchema()

  // get each of the N rows as an array of data
  firstDF.take(10).foreach(println)

  // spark data types - used to describe DF schemas. Spark types are singleton case objects
  val longType: LongType.type = LongType

  // schema is defined as a StructType array of StructFields
  // better to define your own manual schema in production - spark might infer wrong schemas otherwise
  val carsSchema: StructType = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  // obtain the schema from a dataframe
  val carsDFSchema = firstDF.schema

  // reading a schema with own schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // create dataframe rows by hand
  val myRow: Row = Row("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA")

  // create a DF from a sequence of tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )

  // createDataFrame method is heavily overloaded. here, we use the one where a Seq is applied
  // here, schema is auto-inferred because types are already known from the Seq
  // DFs have schemas, rows do not
  val manualCarsDF = spark.createDataFrame(cars)

  // create DF with implicits (?)

  import spark.implicits._

  val manualCarsWithImplicits = cars.toDF(
    "Acceleration", "Cylinders", "Displacement", "Horsepower", "Miles_per_Gallon",
    "Name", "Origin", "Weight_in_lbs", "Year"
  )

  manualCarsDF.printSchema()
  manualCarsWithImplicits.printSchema()
}
