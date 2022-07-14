package sparkdataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  // Joins are wide transformations (expensive operations)

  // best practice to keep the join conditions within a variable
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  // inner joining two dataframes on a common column
  val guitaristsBandsDF: sql.DataFrame = bandsDF.join(
    guitaristsDF,
    joinCondition,
    "inner"
  )

  // outer joining two dataframes
  // left outer join = everything in the inner join + all the rows in the LEFT table,
  // with nulls in where the data is missing
  val leftOuterDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_outer"
  ).show()

  // right outer join = everything in the inner join + all the rows in the RIGHT table,
  // with nulls in where the data is missing
  val rightOuterDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "right_outer"
  ).show()

  // full outer join = everything in the inner join + all the rows in both tables, including nulls
  val fullOuterDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "outer"
  ).show()

  // semi-joins: joins containing data in just one dataframe (either left or right)
  // left semi join: everything in the inner join + only the data in the left dataframe
  // Everything in the left DF for which there is a row in the right DF satisfying the condition
  val leftSemiDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_semi"
  ).show()

  // anti-joins: everything in the left DF for which there is NO row in the right DF satisfying the condition
  val leftAntiDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "left_anti"
  ).show()

  /**
   * Things to bear in mind when running joins:
   *
   * 1. Selecting columns with the same name will crash Spark, e.g. "id" is present in both DFs:
   * {{{
   *   guitaristsBandDF.select("id", "band")
   *     .show()
   * }}}
   *
   * 2. Can use a complex expression as a join condition
   *
   */

  // option 1: join with column renamed
  val guitaristsBandRenamedDF = guitaristsDF.join(
    bandsDF.withColumnRenamed("id", "band"),
    "band"
  )

  // option 2: drop the duplicates after joining - Spark keeps a UID for each column, so call the col from the original DF
  val guitaristsBandsNoDupesDF = guitaristsBandsDF
    .drop(bandsDF.col("id"))

  // option 3: rename the offending column and keep the data, but duplicate data is still present
  val bandsModifiedDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModifiedDF, guitaristsDF.col("band") === bandsDF.col("bandId"))

  // using complex expressions as a join condition
  guitaristsDF.join(
    guitarsDF.withColumnRenamed(
      "id", "guitarId"),
    expr("array_contains(guitars, guitarId)")
  )
}
