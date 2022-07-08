package sparkdataframes

import org.apache.spark.sql.SparkSession

object DFExercises extends App {
  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *  - make
   *  - model
   *  - screen dimension
   *  - camera megapixels
   *
   * 2) Read another file from the data/ folder
   *  - print its schema
   *  - count the number of rows, by calling count()
   */

  // initialize spark session and specify where master node is located
  val spark = SparkSession.builder()
    .appName("DataFrame Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  // create sequence of data tuples
  val phones = Seq(
    ("Apple", "iPhone X", "5\" by 2\"", 1920),
    ("Samsung", "Galaxy S9", "6\" by 3\"", 1234),
    ("Blackberry", "Keyboard Phone", "7\" by 4\"", 1001),
    ("Nokia", "Brick Phone", "8\" by 5\"", 1091)
  )

  // implicits - column names are defined here

  import spark.implicits._

  val phoneDF = phones.toDF("Make", "Model", "Screen Dimension", "Camera Resolution")
  phoneDF.show()
  phoneDF.printSchema()

  // read another dataset
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()
  println(s"Number of rows: ${moviesDF.count()}")
}
