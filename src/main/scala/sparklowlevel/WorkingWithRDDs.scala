package sparklowlevel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WorkingWithRDDs extends App {

  val spark = SparkSession.builder()
    .appName("Working with RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  case class StockValue(symbol: String, date: String, price: Double)

  val stocksRDD: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(row => row.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // Transformations can be done on RDDs, both lazy TRANSFORMATIONS and eager ACTIONS
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // this is a lazy transform
  val msCount = msftRDD.count() // this is an eager action

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // this is lazy

  // min and max - requires implicit ordering because the case class itself cannot be ordered,
  // some field of the cc has to be specified, e.g. price
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  val minMsft: StockValue = msftRDD.min() // is an action

  // reduce
  val numbersRDD = sc.parallelize(1 to 1000000)
  println(numbersRDD.sum())
  println(numbersRDD.reduce(_ + _))

  // grouping - is a very expensive operation, because it involves shuffling data between partitions
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  println(groupedStocksRDD)

  // Partitioning
  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(numPartitions = 30)
  // try writing this 30-partitioned RDD to a parquet file: going to have 30 parts
  // repartitioning is also expensive, as it involves shuffling data
  // BEST PRACTICE: partition early, then process that. Size of a partition should be between [10, 100]MB.
//  repartitionedStocksRDD.toDF().write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/stocks30.parquet")

  // coalesce - doesn't need shuffling
  val coalesceRDD = repartitionedStocksRDD.coalesce(15)

}
