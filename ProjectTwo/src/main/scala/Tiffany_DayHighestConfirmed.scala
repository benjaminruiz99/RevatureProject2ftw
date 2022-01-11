import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.sys.process._

object Tiffany_DayHighestConfirmed {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE TABLE IF NOT EXISTS TotalConfirmed(SNo INT, ObservationDate DATE, ProvinceorState STRING, CountryorRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'covid_data_fixed.csv' OVERWRITE INTO TABLE TotalConfirmed")
//    spark.sql("SELECT * FROM TotalConfirmed").show
    spark.sql("SELECT ObservationDate, SUM(Confirmed) OVER (PARTITION BY (ObservationDate)) AS SumConfirmed FROM TotalConfirmed ORDER BY SumConfirmed").createOrReplaceTempView("Confirmed")
    println("The Date with the highest amount of confirmed cases and how many were confirmed on that day is: ")
    spark.sql("SELECT DISTINCT ObservationDate, FLOOR(MAX(SumConfirmed)) AS MaxConfirmed FROM Confirmed GROUP BY ObservationDate ORDER BY MaxConfirmed DESC LIMIT 1").show(50)

    spark.close()
  }
}