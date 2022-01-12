import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import sys.process._

object Tony_MonthlyTotalCases {

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
    spark.sql("LOAD DATA LOCAL INPATH 'covid_19_data_complete(Kaggle).csv' OVERWRITE INTO TABLE completeData")

    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-y\") AS Month, ProvinceOrState, CountryOrRegion, max(Confirmed) AS MaxCases " +
      " FROM completeData " +
      " GROUP BY Month,CountryOrRegion,ProvinceOrState ").createTempView("monthlyData")

    spark.sql(" SELECT Month, FLOOR(sum(MaxCases)) AS CumulativeMonthlyCases" +
      " FROM monthlyData" +
      " GROUP BY Month" +
      " ORDER BY CumulativeMonthlyCases ASC").show()

    spark.close()
  }
}
