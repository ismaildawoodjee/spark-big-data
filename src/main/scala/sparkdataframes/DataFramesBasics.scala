package sparkdataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

object DataFramesBasics extends App {
  // need a Spark session to run an application on top of Spark
  val spark = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local") // Spark is running on a cluster of local containers
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing/printing out the first N rows of the DF
  firstDF.show()

  // printing the schema of every column in the dataframe
  firstDF.printSchema()

  // get each of the N rows as an array
  firstDF.take(10).foreach(println)

  // spark data types - used to describe DF schemas
  val longType = LongType
}
