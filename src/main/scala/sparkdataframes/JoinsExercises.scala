package sparkdataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsExercises extends App {
  /**
   * Exercises: Use the Postgres database on the Docker container. Write SQL first and translate them to Spark.
   *
   * 1. Show all employees and their max salaries
   *
   * 2. Show all employees who were never managers (not in the `dept_manager` table)
   *
   * 3. Find the job titles of all the most recent, best paid 10 employees in the company
   *
   */

  val spark = SparkSession.builder()
    .appName("Joins Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  /**
   * List of relations
   * Schema |     Name     | Type  | Owner
   * --------+--------------+-------+--------
   * public | departments  | table | docker
   * public | dept_emp     | table | docker
   * public | dept_manager | table | docker
   * public | employees    | table | docker
   * public | movies       | table | docker
   * public | salaries     | table | docker
   * public | titles       | table | docker
   * (7 rows)
   */

  val databaseReader = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5433/rtjvm")
    .option("user", "docker")
    .option("password", "docker")

  // alternatively, define a tableReader function
  def tableReader(tableName: String): sql.DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5433/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$tableName")
      .load()
  }


  /**
   * root
   * |-- emp_no: integer (nullable = true)
   * |-- birth_date: date (nullable = true)
   * |-- first_name: string (nullable = true)
   * |-- last_name: string (nullable = true)
   * |-- gender: string (nullable = true)
   * |-- hire_date: date (nullable = true)
   */
  val employeesDF = databaseReader.option("dbtable", "public.employees").load()
  employeesDF.printSchema()

  /**
   * root
   * |-- emp_no: integer (nullable = true)
   * |-- salary: integer (nullable = true)
   * |-- from_date: date (nullable = true)
   * |-- to_date: date (nullable = true)
   */
  val salariesDF = databaseReader.option("dbtable", "public.salaries").load()
  salariesDF.printSchema()

  /**
   * root
   * |-- dept_no: string (nullable = true)
   * |-- emp_no: integer (nullable = true)
   * |-- from_date: date (nullable = true)
   * |-- to_date: date (nullable = true)
   */
  val deptManagerDF = databaseReader.option("dbtable", "public.dept_manager").load()
  deptManagerDF.printSchema()

  /**
   * root
   * |-- emp_no: integer (nullable = true)
   * |-- title: string (nullable = true)
   * |-- from_date: date (nullable = true)
   * |-- to_date: date (nullable = true)
   */
  val titlesDF = databaseReader.option("dbtable", "public.titles").load()
  titlesDF.printSchema()
  /**
   * 1. SQL statement
   *
   * SELECT
   *   emp.last_name,
   *   MAX(sal.salary) AS max_salary
   * FROM
   *   public.employees AS emp
   * JOIN
   *   public.salaries AS sal ON emp.emp_no = sal.emp_no
   * GROUP BY
   *   emp.last_name
   * ORDER BY
   *   max_salary DESC;
   *
   * Spark statement:
   */
  val empMaxSalJoinCondition = employeesDF.col("emp_no") === salariesDF.col("emp_no")
  val employeeMaxSalary = employeesDF
    .join(salariesDF, empMaxSalJoinCondition, "inner")
    .groupBy(employeesDF.col("last_name"), employeesDF.col("emp_no"))
    .agg(max(salariesDF.col("salary")).as("max_salary"))
    .orderBy(col("max_salary").desc_nulls_last)

  /**
   * 2. SQL statement (if they are never managers, what salary and titles do they have?)
   *
   * SELECT
   *   last_name,
   *   title,
   *   max(salary) AS max_salary
   * FROM
   *   employees AS e
   * JOIN
   *   dept_manager AS dm ON e.emp_no != dm.emp_no
   * JOIN
   *   title AS t ON e.emp_no = t.emp_no
   * JOIN
   *   salaries AS s ON e.emp_no = s.emp_no
   * GROUP BY
   *   last_name, title
   * ORDER BY
   *   max_salary DESC;
   *
   * Spark statement: could also use an anti-join
   */
  val empNevMgrJoinCondition = employeesDF.col("emp_no") =!= deptManagerDF.col("emp_no")
  val empNeverManager = employeesDF
    .join(deptManagerDF, empNevMgrJoinCondition, joinType = "inner")
    .show()

  /**
   * 3. SQL statement:
   *
   * SELECT
   *   last_name,
   *   title,
   *   MAX(salary) AS max_salary
   * FROM
   *   employees AS e
   * JOIN
   *   titles AS t ON e.emp_no = t.emp_no
   * JOIN
   *   salaries AS s ON e.emp_no = s.emp_no
   * GROUP BY
   *   emp_no, title
   * ORDER BY
   *   max_salary DESC
   * LIMIT 10;
   *
   * Spark statement:
   */

  val recBestPaidJobJoinCondition = employeesDF.col("emp_no") === salariesDF.col("emp_no")
  val bestPaidTitlesJoinCondition = employeesDF.col("emp_no") === titlesDF.col("emp_no")
  val recentBestPaidJobTitlesDF = employeesDF
    .join(salariesDF, recBestPaidJobJoinCondition, joinType = "inner")
    .join(titlesDF, bestPaidTitlesJoinCondition, joinType = "inner")
    .groupBy(employeesDF.col("last_name"), salariesDF.col("salary"), titlesDF.col("title"))
    .agg(max(salariesDF.col("salary")).as("max_salary"))
    .orderBy(col("max_salary").desc_nulls_last)
    .limit(20)
    .show()

  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeeMaxSalary.orderBy(col("max_salary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()

}
