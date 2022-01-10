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

    val TotalConfirmedDF = spark.read.csv("C:\\Data\\Kaggle\\covid_19_data_complete(Kaggle).csv")
      .toDF("SNo",
        "ObservationDate",
        "ProvinceorState",
        "CountryorRegion",
        "LastUpdate",
        "Confirmed",
        "Deaths",
        "Recovered"
      )
      val TotalConfirmedDF2 = TotalConfirmedDF
        .withColumn("SNo",col("SNo").cast("int"))
        .withColumn("ObservationDate",col("ObservationDate").cast("date"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))
        .withColumn("ProvinceorState",col("ProvinceorState").cast("String"))

    "CountryorRegion",
      "LastUpdate",
      "Confirmed",
      "Deaths",
      "Recovered"
    )
    TotalConfirmedDF.show()

    DF = spark.read.csv("column1","column2").select(col("column1")).cast("int").as......  ??

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
      "LOAD DATA LOCAL INPATH 'covid_19_data_complete(Kaggle).csv'" +
      "OVERWRITE INTO TABLE TotalConfirmed"
    )

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

    spark.sql("SELECT SNo," +
      "date_format(ObservationDate, 'yyyy-mm-dd')," +
      "ProvinceorState," +
      "CountryorRegion," +
      "LastUpdate," +
      "Confirmed," +
      "Deaths," +
      "Recovered" +
      "FROM TotalConfirmed").show()



//    Brady: cumulative case-fatality ratio over time, by country
    spark.sql("SELECT CountryorRegion, ObservationDate," +
      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
      "FROM TotalConfirmed GROUP BY 1, 2 ORDER BY 3 DESC").show()

    spark.close()
  }
}
