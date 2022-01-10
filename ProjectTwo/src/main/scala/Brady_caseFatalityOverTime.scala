import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._

import sys.process._

object Brady_caseFatalityOverTime {

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
      "ObservationDate DATE," +
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
      "LOAD DATA LOCAL INPATH 'covid_19_data_fixed.csv'" +
      "OVERWRITE INTO TABLE TotalConfirmed"
    )

////  Brian: total confirmed cases.
//    spark.sql("SELECT FLOOR(sum(Confirmed)) as TotalConfirmed FROM TotalConfirmed")
//      .show()
//
////    Brady: case-fatality ratio by country
//    spark.sql("SELECT CountryorRegion, " +
//      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
//      "FROM TotalConfirmed GROUP BY CountryorRegion ORDER BY 2 DESC").show()
//
////    Ben: country with most cases
//    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
//    temp_table.registerTempTable("MaxCasesByStateCountry")
//    println("The country with most cases and how many cases")
//    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show()

//    Brady: cumulative case-fatality ratio by country by month
    spark.sql("SELECT CountryorRegion, date_format(ObservationDate,'M-y') AS Month," +
      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
      "FROM TotalConfirmed GROUP BY 1, 2 ORDER BY 1, 2").show()

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.registerTempTable("MaxCasesByStateCountry")
    println("The country with most cases and how many cases")
    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show
    spark.close()


    spark.close()
  }
}
