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

    spark.sql("CREATE TABLE IF NOT EXISTS TotalConfirmed(SNo INT, ObservationDate DATE, ProvinceorState STRING, CountryorRegion STRING, " +
      "LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'covid_data_fixed.csv' OVERWRITE INTO TABLE TotalConfirmed")
    spark.sql("SELECT ObservationDate, MAX(Confirmed) AS MaxConfirmed FROM TotalConfirmed " +
      "GROUP BY ObservationDate ORDER BY MaxConfirmed").createOrReplaceTempView("Confirmed")
    spark.sql("SELECT ObservationDate as Date, FLOOR(SUM(MaxConfirmed)) AS MaxConfirmed " +
      "FROM Confirmed GROUP BY Date ORDER BY MaxConfirmed").createOrReplaceTempView("DailyConfirmed")
    spark.sql("SELECT Date, MaxConfirmed, LAG(MaxConfirmed,1) OVER (ORDER BY MaxConfirmed ASC) " +
      "AS PreviousMaxConfirmed FROM DailyConfirmed ORDER BY MaxConfirmed ASC").createOrReplaceTempView("DailyMaxConfirmed")
    spark.sql("SELECT DATE_FORMAT(Date,\"M-d-y\") AS Date, MaxConfirmed - IFNULL(PreviousMaxConfirmed,0) AS Total" +
      " FROM DailyMaxConfirmed ORDER BY Total DESC LIMIT 1").show

    spark.close()
  }
}