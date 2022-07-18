package sparkbigdataproject

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TaxiEconomicImpact {
  /**
   * We assume some parameters for estimating potential economic impact of grouping taxi rides:
   *
   * 5% of taxi trips detected to be group-able,
   *
   * but only 30% of people actually accept to be grouped.
   *
   * Get a $5 discount if you take a grouped ride,
   *
   * and incur an extra $2 if you take an individual ride.
   *
   * If two rides are grouped, assume that cost of an average ride is reduced by 60%
   *
   */
  def main(args: Array[String]): Unit = {
    /**
     * To package this app as a jar file, read data from S3 and process using EMR, the cmd arguments are:
     *
     * 1. big data source on S3 (35 GB parquet files)
     *
     * 2. taxi zones data source, also on S3
     *
     * 3. output destination for writing final economic impact number
     *
     * Exact spark-submit command, after copying the jar file into EMR master node:
     *
     * spark-submit \
     * --class sparkbigdataproject.TaxiEconomicImpact \
     * --supervise \
     * --verbose spark-big-data.jar \
     * s3://sparkproject-data/taxi-bigdata s3://sparkproject-data/taxi_zones.csv s3://sparkproject-data/economicImpact.csv
     *
     */
    if (args.length != 3) {
      println("Need a 1) big data source, 2) taxi zones data source, and 3) output data destination")
      sys.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Taxi Big Data Application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxiDF = spark.read.load(args(0))

    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discountInDollars = 5
    val extraCostInDollars = 2
    val percentGroupable = 289623.0 / 331893

    val groupAttemptsDF = taxiDF
      .select(
        round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
        col("pickup_taxizone_id"),
        col("total_amount"))
      .groupBy(
        col("fiveMinId"),
        col("pickup_taxizone_id"))
      .agg(
        (count("*") * percentGroupable).as("totalTrips"),
        round(sum(col("total_amount"))).as("totalAmount"))
      .withColumn("approximateDatetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
      .drop("LocationID", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)

    val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount")).as[Double]).take(1)(0)

    val groupingEstimateEconomicImpactDF = groupAttemptsDF
      .withColumn("groupedRides",
        col("totalTrips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomicImpact",
        col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discountInDollars))
      .withColumn("rejectedGroupedRidesEconomicImpact",
        col("groupedRides") * (1 - percentAcceptGrouping) * extraCostInDollars)
      .withColumn("totalImpact",
        col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

    val totalEconomicProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))

    totalEconomicProfitDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(args(2))
  }
}
