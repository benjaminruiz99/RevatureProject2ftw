import org.apache.spark.sql.SparkSession

object DataPractice_BMCC {
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

    spark.sql("CREATE TABLE IF NOT EXISTS TotalConfirmed(SNo INT, ObservationDate STRING, ProvinceorState STRING, CountryorRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql(s"LOAD DATA LOCAL INPATH \'covid_19_data_complete(Kaggle).csv\' OVERWRITE INTO TABLE TotalConfirmed")

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.registerTempTable("MaxCasesByStateCountry")
    println("The country with most cases and how many cases")
    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show
    spark.close()
  }
}
