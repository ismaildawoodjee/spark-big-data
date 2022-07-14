package sparktypesanddatasets

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

object Datasets extends App {
  /** Datasets are essentially typed dataframes (a distributed collection of JVM objects)
   * Dataframes are distributed collection of rows
   */
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  // add more type information to a DF and transform it to a DS
  // requires an implicit encoder to convert DF into a typed DS
  // an Encoder[Int] value has the capability of turning a row in a DF into an Int which will be used in the DS
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // numbersDF.filter(_ < 100) // does not work
  numbersDF.filter(col("numbers") < 100)
  // with the Dataset, we can now use the filter operation like we would use it on a Seq of Ints
  numbersDS.filter(_ < 100)

  // for dataframes with multiple columns, use a `case object`
  // dataset of a complex type

  /**
   * To define a dataset,
   *
   * 1. Define the case class with fields corresponding to the column names and types in the Dataframe
   *
   * 2. Read the Dataframe from the file
   *
   * 3. Define an encoder, which will be taken care of by just importing spark.implicits_
   *
   * 4. Finally, convert the Dataframe to Dataset using the defined case class
   */
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  def readJSONtoDF(filename: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")
  }

  val carsDF = readJSONtoDF("cars.json")

  // Encoders.product will take any type that extends the `product` type (all `case class`es extend the `product` type)
  // it helps Spark to identify which fields in the case class map to which column in the DF

  // just import spark implicits, no need to write the following
  // implicit val  carsEncoder: Encoder[Car] = Encoders.product[Car]

  import spark.implicits._

  val carsDS: Dataset[Car] = carsDF.as[Car]

  // once a dataframe becomes a dataset, we can operate on datasets just like any collection
  numbersDS.filter(_ < 100)

  // can use map, flatMap, reduce, filter, fold, for comprehensions, etc.,...
  val carNamesDS: Dataset[String] = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  /**
   * Datasets are most useful when
   *
   * 1. we want to maintain type information
   *
   * 2. we want to maintain clear and concise code, especially in prod environments
   *
   * 3. our filters/transformations are hard to express with DFs and SQL
   *
   * Disadvantage:
   *
   * -  when performance is critical, Spark can't optimize transformations done in Datasets.
   * need to do transforms on a row-by-row basis, which is really slow.
   *
   */

  // DATASET JOINS
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[String], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readJSONtoDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readJSONtoDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readJSONtoDF("bands.json").as[Band]

  // a dataset of tuples will be produced
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(
      bandsDS,
      guitarPlayersDS.col("band") === bandsDS.col("id"),
      joinType = "inner"
    )
  guitarPlayerBandsDS.show()

  /**
   * Exercise: join guitarsDS and guitarPlayersDS (use array_contains) with an outer join
   */
  val guitarsGuitarPlayersDS: Dataset[(Guitar, GuitarPlayer)] = guitarsDS
    .joinWith(
      guitarPlayersDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      joinType = "outer"
    )
  guitarsGuitarPlayersDS.show()

  // GROUPING DATASETS - for each row in the dataset, use the Origin as the key for groupBy operation
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count
    .as("carsOriginCount")
  carsGroupedByOrigin.show()
}
