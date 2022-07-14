package sparkdataframes

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, SparkSession}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // creating columns - new columns out of existing columns
  val firstColumn: Column = carsDF.col("Name")

  // selecting columns - technical term is called "projection"
  val carNamesDF: sql.DataFrame = carsDF.select(firstColumn)

  // specify multiple columns
  carsDF.select(
    carsDF.col("Name"),
    carsDF.col("Acceleration")
  )

  import spark.implicits._

  // alternatively (cannot interchange between these different selection methods)
  carsDF.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // single quote at the prefix is a Scala symbol - auto-converted to column
    $"Horsepower", // also returns a column object
    expr("Origin") // an expression construct
  ).show()
  // select with plain column names
  carsDF.select("Name", "Year")

  // selecting column names is an example of an EXPRESSION in Spark
  val simpleExpression: Column = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = carsDF.col("Weight_in_lbs") / 2.2

  // using expression in a "select" statement. Set new expr name with .as("new_name") method
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
    // pass in a SQL-like string to construct an expression, instead of the above
  )

  carsWithWeightsDF.show()

  // selectExpr - select, but with expr calls inside
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
  )

  carsWithSelectExprWeightsDF.show()

  // DataFrame processing
  // adding a new column using "withColumn" and obtain a new dataframe
  val carsWithWeightKg: sql.DataFrame = carsDF.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
  carsWithWeightKg.show()

  // renaming a column
  val carsWithColumnRenamed: sql.DataFrame = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  carsWithColumnRenamed.show()

  // selecting columns with spaces - use backticks
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()

  // remove a column
  val carsWithColumnRemoved = carsWithColumnRenamed.drop("Cylinders", "Displacement")
  carsWithColumnRemoved.show()

  // Filtering
  // filter all cars that are non-USA origin (inequality is =!=)
  val nonUsaCarsDF = carsDF.filter(col("Origin") =!= "USA")

  // "where" is exactly the same (equality is triple equals sign ===)
  val usaCarsOnlyDF = carsDF.where(col("Origin") === "USA")
  nonUsaCarsDF.show()

  // filtering with expression strings
  val americanCarDF = carsDF.filter("Origin = 'USA'")

  // chaining filters
  val usaPowerfulCarsDF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  val usaPowerfulCarsDF2 = carsDF
    .filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val usaPowerfulCarsDF3 = carsDF
    .filter(col("Origin") === "USA" and col("Horsepower") > 150) // since "and" can be used as an infix operator

  // best method - to use SQL-like syntax in the filter condition expression
  val usaPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union - add more rows by sticking two DFs together. Only works if both have the same schema
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  // get distinct values from a specific column
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

}
