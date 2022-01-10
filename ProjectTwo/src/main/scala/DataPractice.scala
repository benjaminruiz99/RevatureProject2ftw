import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.sys.process._

object DataPractice {

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
//    spark.sql("SELECT FLOOR(sum(Confirmed)) as TotalConfirmed FROM TotalConfirmed").show()
    spark.sql("SELECT MAX(Recovered) as TotalRecovered FROM TotalConfirmed GROUP BY ProvinceorState, CountryorRegion").createOrReplaceTempView("Recovered")
    spark.sql("SELECT FLOOR(SUM(TotalRecovered)) as TotalRecovered FROM Recovered").show
//    val dfConfirmedUS = spark.read.option("header", true).csv("time_series_covid_19_confirmed_US_complete(Kaggle).csv").createOrReplaceTempView("ConfirmedCompleteUS")
//    spark.sql("SELECT UID, iso2, iso3 FROM ConfirmedCompleteUS").show
////    spark.sql("SELECT MAX(")
//    val dfConfirmedWorld = spark.read.option("header", true).csv("time_series_covid_19_confirmed.csv").createOrReplaceTempView("ConfirmedWorld")
//    spark.sql("SELECT Province_State, Country_Region, Lat, Jan_22_2020 FROM ConfirmedWorld").show
//    spark.sql("SELECT ObservationDate, SUM(Confirmed) as Total FROM TotalConfirmed GROUP BY ObservationDate").createOrReplaceTempView("ConfirmedTotal")
//    spark.sql("SELECT ObservationDate, MAX(Total) as Max FROM ConfirmedTotal GROUP BY ").show
//    spark.sql("SELECT * FROM ConfirmedTotal").show

    spark.close()
  }
}