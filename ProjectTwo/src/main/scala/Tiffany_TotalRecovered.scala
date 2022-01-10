import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.sys.process._

object Tiffany_TotalRecovered {

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
    spark.sql("SELECT MAX(Recovered) as TotalRecovered FROM TotalConfirmed GROUP BY ProvinceorState, CountryorRegion").createOrReplaceTempView("Recovered")
    spark.sql("SELECT FLOOR(SUM(TotalRecovered)) as TotalRecovered FROM Recovered").show

    spark.close()
  }
}