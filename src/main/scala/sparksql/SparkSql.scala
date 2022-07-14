package sparksql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql extends App {

  // specify config here to change directory of spark-warehouse - could be S3, HDFS, somewhere else, etc
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // selecting with the regular dataframe API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // Spark SQL API - will create a temporary view with alias
  carsDF.createOrReplaceTempView("cars")
  val usaCarsDF = spark.sql(
    """
      | SELECT
      |   name
      | FROM
      |   cars
      | WHERE
      |   origin = 'USA'
      |""".stripMargin
  )

  // can also create databases, tables, etc - can perform DDL statements with Spark SQL
  // this creates a rtjvm.db file at the root directory within the `spark-warehouse` folder
  // to change the directory of the warehouse folder, specify it in SparkSession config
  spark.sql("CREATE DATABASE rtjvm")

  // we can run any SQL statement
  spark.sql("USE rtjvm")
  val databasesDF = spark.sql("SHOW DATABASES")
  databasesDF.show()

  /**
   * How to transfer tables from a database to Spark tables?
   */
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5433/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()
  }

  val employeesDF = readTable("employees")
  // transfer the employeesDF to a Spark SQL table
  //  employeesDF.write
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("employees")

  // transfer all tables in Postgres into Spark Warehouse
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  ))

  // read the table from Spark warehouse as a dataframe
  val employeesTable: DataFrame = spark.read.table("employees")
  employeesTable.show()

  /**
   * Exercises:
   *
   * 1. Read the moviesDF and store it as a Spark table in the rtjvm database.
   *
   * 2. Count how many employees were hired in between Jan 1 2000 and Jan 1 2001.
   *
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   *
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   *
   */

  spark.sql(
    """
      |SELECT
      |  COUNT(*) AS num_employees
      |FROM
      |  employees
      |WHERE
      |  hire_date BETWEEN '1999-01-01' AND '2000-01-01';
      |""".stripMargin
  ).show()

  spark.sql(
    """
      |SELECT
      |  AVG(s.salary) AS avg_salary,
      |  d.dept_name
      |FROM
      |  employees AS e
      |JOIN
      |  salaries AS s ON e.emp_no = s.emp_no
      |JOIN
      |  dept_emp AS de ON e.emp_no = de.emp_no
      |JOIN
      |  departments AS d ON de.dept_no = d.dept_no
      |WHERE
      |  hire_date BETWEEN '1999-01-01' AND '2000-01-01'
      |GROUP BY
      |  d.dept_name
      |ORDER BY
      |  avg_salary DESC
      |LIMIT 10;
      |""".stripMargin
  ).show()
}
