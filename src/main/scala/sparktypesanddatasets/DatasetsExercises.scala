package sparktypesanddatasets

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetsExercises extends App {
  /**
   * Exercises - use dataset only:
   *
   * 1. Count how many cars we have
   *
   * 2. Count how many powerful cars we have (horsepower > 140)
   *
   * 3. Average horsepower of the entire dataset
   *
   */
  val spark = SparkSession.builder()
    .appName("Datasets Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  // 1. Define case class
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

  // 2. Read dataframe
  val carsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/cars.json")

  // 3. Import implicits

  import spark.implicits._

  // 4. Convert DF to DS
  val carsDS: Dataset[Car] = carsDF.as[Car]

  // Ex. 1
  val carCount: Long = carsDS.count
  println(s"There are a total of: $carCount cars")
  println()

  // Ex. 2
  val powerfulCarCount: Long = carsDS.filter("Horsepower > 140").count
  // alternatively, use getOrElse because Horsepower is an Option type; default is 0 if Horsepower is null
  val powerfulCarCountAlt = carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
  println(s"There are only $powerfulCarCount powerful cars")
  println()

  // Ex. 3
  val avgHorsepower = carsDS.selectExpr("avg(Horsepower) as avgHorsepower")
  // alternatively, use map to get HP values and then reduce to sum them up
  val avgHorsepowerAlt = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carCount
  avgHorsepower.show()
  println()
}
