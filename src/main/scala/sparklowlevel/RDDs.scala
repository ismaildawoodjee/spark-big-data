package sparklowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.io.Source // for manually reading from an input (file/URL, etc)), line by line as an iteration

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // before working with RDDs, we need a Spark Context
  val sc = spark.sparkContext

  /** Ways to create an RDD:
   *
   * 1. Parallelize an existing collection
   *
   * 2. Reading from files and parallelize it
   *
   * 2.b Read using the .textFile method and processing each line again as above
   *
   * 3. Convert DataFrame to DataSet, then convert DataSet to RDD
   * DF -> DS -> RDD
   *
   */

  // 1 - parallelize: distribute a collection of numbers across multiple nodes
  val numbers: Seq[Int] = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2 - reading from files: e.g. a CSV, read each line in a CSV, split by "," and then convert to a list
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String): List[StockValue] = {
    Source.fromFile(filename)
      .getLines()
      .drop(1) // drop the header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksList: Seq[StockValue] = readStocks("src/main/resources/data/stocks.csv")
  val stocksRDD: RDD[StockValue] = sc.parallelize(stocksList)

  // 2b - another way of reading is using .textFile method
  // the RDD has already been distributed, so we don't know where the header is located to drop it
  // will need to filter it manually with a certain condition
  val stocksListAlt: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS: Dataset[StockValue] = stocksDF.as[StockValue]
  val stocksRDD2: RDD[StockValue] = stocksDS.rdd

  // can also convert RDD to a DF
  val numbersDF: DataFrame = numbersRDD.toDF("numbers")
  val stocksDFFromRDD: DataFrame = stocksRDD.toDF("symbol", "date", "price")
  stocksDFFromRDD.show()
  stocksDFFromRDD.printSchema()

  // convert RDD to DS
  val numbersDSFromRDD = spark.createDataset(numbersRDD)
  val stocksDSFromRDD = spark.createDataset(stocksRDD)
  stocksDSFromRDD.show()
  stocksDSFromRDD.printSchema()

  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(numPartitions = 30)
  repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/stocks30.parquet")
}
