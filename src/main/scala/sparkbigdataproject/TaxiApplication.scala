package sparkbigdataproject

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication {
  /**
   * Questions to ask about the taxi dataset:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   *
   * 2  Which boroughs in NY have the most pickups/dropoffs?
   *
   * 3. What are the peak hours for taxi rides?
   *
   * 4. How are the trips distributed by length? Why are people taking the cab?
   *
   * 5. What are the peak hours for long/short trips?
   *
   * 6. What are the top 3 pickup/dropoff zones for long/short trips?
   *
   * 7. How are people paying for the ride, on long/short trips?
   *
   * 8. How is the payment type evolving with time?
   *
   * 9. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Taxi Big Data Application")
      .config("spark.master", "local")
      .getOrCreate()

    /** taxiDF Schema
     * |-- VendorID: integer (nullable = true)
     * |-- tpep_pickup_datetime: timestamp (nullable = true)
     * |-- tpep_dropoff_datetime: timestamp (nullable = true)
     * |-- passenger_count: integer (nullable = true)
     * |-- trip_distance: double (nullable = true)
     * |-- RatecodeID: integer (nullable = true)
     * |-- store_and_fwd_flag: string (nullable = true)
     * |-- PULocationID: integer (nullable = true)
     * |-- DOLocationID: integer (nullable = true)
     * |-- payment_type: integer (nullable = true)
     * |-- fare_amount: double (nullable = true)
     * |-- extra: double (nullable = true)
     * |-- mta_tax: double (nullable = true)
     * |-- tip_amount: double (nullable = true)
     * |-- tolls_amount: double (nullable = true)
     * |-- improvement_surcharge: double (nullable = true)
     * |-- total_amount: double (nullable = true)
     */
    val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

    /** taxiZonesDF Schema
     * |-- LocationID: integer (nullable = true)
     * |-- Borough: string (nullable = true)
     * |-- Zone: string (nullable = true)
     * |-- service_zone: string (nullable = true)
     */
    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/taxi_zones.csv")

    // 1. Which zones have the most pickups/dropoffs overall?
    val taxiAndZoneJoinCondition = taxiDF.col("PULocationID") === taxiZonesDF.col("LocationID")
    val pickupsByTaxiZone = taxiDF
      .groupBy(col("PULocationID"))
      .agg(count("*").as("pickupCount"))
      .join(taxiZonesDF, taxiAndZoneJoinCondition)
      .orderBy(col("pickupCount").desc_nulls_last)
    //    pickupsByTaxiZone.show()
    // Manhattan, mainly. Confirm with 2)

    // 2. Which boroughs in NY have the most pickups/dropoffs?
    val pickupsByBorough = pickupsByTaxiZone
      .groupBy(col("Borough"))
      .agg(sum("pickupCount").as("pickupCount"))
      .orderBy(col("pickupCount").desc_nulls_last)
    //    pickupsByBorough.show()
    /**
     * +-------------+-----------+
     * |      Borough|pickupCount|
     * +-------------+-----------+
     * |    Manhattan|     304266|
     * |       Queens|      17712|
     * |      Unknown|       6644|
     * |     Brooklyn|       3037|
     * |        Bronx|        211|
     * |          EWR|         19|
     * |Staten Island|          4|
     * +-------------+-----------+
     * Data is extremely skewed towards Manhattan. Could increase prices for Manhattan and decrease prices for others.
     */

    // 3. What are the peak hours for taxi rides? Need to extract hour from tpep_pickup_datetime
    val peakHours = taxiDF
      .groupBy(hour(col("tpep_pickup_datetime")).as("hourOfDay"))
      .agg(count("*").as("peakHoursPickupCount"))
      .orderBy(col("peakHoursPickupCount").desc_nulls_last)
    //    peakHours.show(24)
    // This is a random sample of the dataset, so peak hour results were not consistent
    // However, can adjust prices like the previous situation: higher during peak hours and lower during other times.

    // 4. How are the trips distributed by length? Why are people taking the cab?
    val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
    val longDistanceThreshold = 30 // 30 miles and above will be considered a long trip
    val tripDistanceStatsDF = tripDistanceDF
      .select(
        count("*").as("count"),
        lit(longDistanceThreshold).as("threshold"),
        mean("distance").as("meanDistance"),
        stddev("distance").as("stddevDistance"),
        min("distance").as("min"),
        max("distance").as("max"))
    tripDistanceStatsDF //.show()

    /**
     * +------+---------+-----------------+-----------------+---+----+
     * | count|threshold|     meanDistance|   stddevDistance|min| max|
     * +------+---------+-----------------+-----------------+---+----+
     * |331893|       30|2.717989442380494|3.485152224885052|0.0|66.0|
     * +------+---------+-----------------+-----------------+---+----+
     */
    // Alternatively,
    taxiDF.describe("trip_distance") //.show()

    /**
     * +-------+-----------------+
     * |summary|    trip_distance|
     * +-------+-----------------+
     * |  count|           331893|
     * |   mean|2.717989442380494|
     * | stddev|3.485152224885052|
     * |    min|              0.0|
     * |    max|             66.0|
     * +-------+-----------------+
     */
    val tripsWithLengthDF = taxiDF
      .withColumn("isLongTrip", col("trip_distance") >= longDistanceThreshold)
    val tripsByLengthDF = tripsWithLengthDF
      .groupBy("isLongTrip")
      .agg(count("*").as("tripCount"))
      .orderBy(col("tripCount").desc_nulls_last)
    //    tripsByLengthDF.show()
    // Overwhelming majority of trips are short trips
    /**
     * +----------+---------+
     * |isLongTrip|tripCount|
     * +----------+---------+
     * |     false|   331810|
     * |      true|       83|
     * +----------+---------+
     */

    // 5. What are the peak hours for long/short trips?
    val peakHoursByLengthDF = tripsWithLengthDF
      .groupBy(
        hour(col("tpep_pickup_datetime")).as("hourOfDay"),
        col("isLongTrip"))
      .agg(count("*").as("peakHoursPickupCount"))
      .orderBy(col("peakHoursPickupCount").desc_nulls_last)
    //    peakHoursByLengthDF.show(48)

    /**
     * +---------+----------+--------------------+
     * |hourOfDay|isLongTrip|peakHoursPickupCount|
     * +---------+----------+--------------------+
     * ...
     * |        4|     false|               10853|
     * |       10|     false|                5358|
     * |        5|     false|                5124|
     * |        6|     false|                3194|
     * |        7|     false|                1971|
     * |        9|     false|                1803|
     * |        8|     false|                1565|
     * |        1|      true|                   9|
     */

    // 6. What are the top 3 pickup/dropoff zones for long/short trips?
    def mostPopularPUDOPoints(predicate: Column) = tripsWithLengthDF
      .where(predicate)
      .groupBy("PULocationID", "DOLocationID")
      .agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "PickupZone")
      .drop("LocationID", "Borough", "service_zone")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "DropoffZone")
      .drop("Location", "Borough", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)
    //    mostPopularPUDOPoints(not(col("isLongTrip"))).show()
    //    mostPopularPUDOPoints(col("isLongTrip")).show()
    // Short trips are between wealthy zones
    /**
     * +------------+------------+----------+--------------------+----------+--------------------+
     * |PULocationID|DOLocationID|totalTrips|          PickupZone|LocationID|         DropoffZone|
     * +------------+------------+----------+--------------------+----------+--------------------+
     * |         264|         264|      5558|                  NV|       264|                  NV|
     * |         237|         236|      2425|Upper East Side S...|       236|Upper East Side N...|
     * |         236|         237|      1962|Upper East Side N...|       237|Upper East Side S...|
     * |         236|         236|      1944|Upper East Side N...|       236|Upper East Side N...|
     * |         237|         237|      1928|Upper East Side S...|       237|Upper East Side S...|
     * |         237|         161|      1052|Upper East Side S...|       161|      Midtown Center|
     * |         237|         162|      1012|Upper East Side S...|       162|        Midtown East|
     * |         161|         237|       987|      Midtown Center|       237|Upper East Side S...|
     * |         239|         238|       965|Upper West Side S...|       238|Upper West Side N...|
     */
    // Long trips are between airports, etc.
    /**
     * +------------+------------+----------+--------------------+----------+--------------------+
     * |PULocationID|DOLocationID|totalTrips|          PickupZone|LocationID|         DropoffZone|
     * +------------+------------+----------+--------------------+----------+--------------------+
     * |         132|         265|        14|         JFK Airport|       265|                  NA|
     * |         138|         265|         8|   LaGuardia Airport|       265|                  NA|
     * |         132|         132|         4|         JFK Airport|       132|         JFK Airport|
     * |         132|           1|         4|         JFK Airport|         1|      Newark Airport|
     * |         264|         264|         3|                  NV|       264|                  NV|
     * |         164|         265|         3|       Midtown South|       265|                  NA|
     * |         163|           1|         2|       Midtown North|         1|      Newark Airport|
     * |         132|         200|         2|         JFK Airport|       200|Riverdale/North R...|
     */

    // 7. How are people paying for the ride, on long/short trips?
    val paymentOnTrips = tripsWithLengthDF
      .groupBy(col("RatecodeID"), col("isLongTrip"))
      .agg(count("*").as("tripCount"))
      .orderBy(col("tripCount").desc_nulls_last)
    //    paymentOnTrips.show()
    /**
     * Overwhelmingly paid by credit cards (1) and then cash (2)
     * +----------+----------+---------+
     * |RatecodeID|isLongTrip|tripCount|
     * +----------+----------+---------+
     * |         1|     false|   324362|
     * |         2|     false|     5871|
     * |         5|     false|      870|
     * |         3|     false|      522|
     * |         4|     false|      175|
     * |         1|      true|       25|
     * |         5|      true|       25|
     * |         4|      true|       18|
     * |         3|      true|        8|
     * |         2|      true|        7|
     * |        99|     false|        7|
     * |         6|     false|        3|
     * +----------+----------+---------+
     */

    // 8. How is the payment type evolving with time? Extract day from the tpep_pickup_datetime column and groupby it
    val ratecodeEvolutionByTime = taxiDF
      .groupBy(
        to_date(col("tpep_pickup_datetime")).as("pickup_day"),
        col("RatecodeID"))
      .agg(count("*").as("totalTrips"))
      .orderBy(col("totalTrips").desc_nulls_last)
    //    ratecodeEvolutionByTime.show()

    // 9. Can we explore a ride-sharing opportunity by grouping close short trips?
    // take datetime col, and convert it to 5 min bucket ID, and study how
    val groupAttemptsDF = taxiDF
      .select(
        round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
        col("PULocationID"),
        col("total_amount"))
      .where(col("passenger_count") < 3)
      .groupBy(
        col("fiveMinId"),
        col("PULocationID"))
      .agg(
        count("*").as("totalTrips"),
        round(sum(col("total_amount"))).as("totalAmount"))
      .withColumn("approximateDatetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .drop("LocationID", "service_zone")
      .orderBy(col("totalTrips").desc_nulls_last)
    groupAttemptsDF.show()


  }
}
