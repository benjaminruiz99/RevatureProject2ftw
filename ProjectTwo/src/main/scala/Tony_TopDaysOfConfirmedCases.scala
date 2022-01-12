import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import sys.process._

object Tony_TopDaysOfConfirmedCases {

  def main(args: Array[String]): Unit = {


    System.setProperty("  hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("DROP TABLE IF EXISTS completeData")
    spark.sql("CREATE TABLE IF NOT EXISTS completeData(SNo INT, ObservationDate DATE, ProvinceOrState STRING, CountryOrRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'covid_data_fixed.csv' OVERWRITE INTO TABLE completeData")

    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-d-y\") AS Day, ProvinceOrState, CountryOrRegion, max(Confirmed) AS MaxCases " +
      " FROM completeData " +
      " GROUP BY Day,CountryOrRegion,ProvinceOrState ").createTempView("dailyData")

    spark.sql(" SELECT Day, FLOOR(sum(MaxCases)) AS CumulativeDailyCases" +
      " FROM dailyData" +
      " GROUP BY Day" +
      " ORDER BY CumulativeDailyCases ASC").createTempView("DailyCumulative")

    spark.sql(" SELECT Day, CumulativeDailyCases, " +
      " LAG(CumulativeDailyCases,1) OVER(" +
      " ORDER BY CumulativeDailyCases ASC) AS PreviousDailyCases" +
      " FROM dailyCumulative" +
      " ORDER BY CumulativeDailyCases ASC").createTempView("dailyCumulative2")

    spark.sql(" SELECT Day, CumulativeDailyCases - IFNULL(PreviousDailyCases,0) AS NewCases" +
      " FROM dailyCumulative2" +
      " ORDER BY NewCases DESC" +
      " LIMIT 10").show()

    spark.close()
  }
}
