import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._

object BMcK_totalDeathsandRecoveries {

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

    spark.sql("DROP TABLE IF EXISTS TotalConfirmed")
    spark.sql("CREATE TABLE TotalConfirmed(" +
      "SNo INT," +
      "ObservationDate STRING," +
      "ProvinceorState STRING," +
      "CountryorRegion STRING," +
      "LastUpdate STRING," +
      "Confirmed DOUBLE," +
      "Deaths DOUBLE," +
      "Recovered DOUBLE)" +
      "row format delimited fields terminated by ','" +
      "stored as textfile"
    )

    spark.sql(
      "LOAD DATA LOCAL INPATH 'covid_19_data_complete(Kaggle).csv'" +
        "OVERWRITE INTO TABLE TotalConfirmed"
    )

    spark.sql("SELECT FLOOR(sum(Confirmed)) as TotalConfirmed FROM TotalConfirmed")
      .show()

    spark.sql("SELECT CountryorRegion, " +
      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
      "FROM TotalConfirmed GROUP BY CountryorRegion ORDER BY 2 DESC").show()

    spark.sql("SELECT MAX(Deaths) as TotalConfirmedDeaths FROM TotalConfirmed GROUP BY CountryorRegion").createTempView("TempTCD")
    spark.sql("SELECT SUM(TotalConfirmedDeaths) FROM TempTCD").show()

    spark.sql("SELECT MAX(Recovered) as TotalConfirmedRecoveries FROM TotalConfirmed GROUP BY CountryorRegion").createTempView("TempTCR")
    spark.sql("SELECT FLOOR(SUM(TotalConfirmedRecoveries)) FROM TempTCR").show()

  }
}
