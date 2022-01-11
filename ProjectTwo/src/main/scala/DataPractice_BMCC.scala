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
    spark.sql(s"LOAD DATA LOCAL INPATH \'covid_data_fixed.csv\' OVERWRITE INTO TABLE TotalConfirmed")

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.registerTempTable("MaxCasesByStateCountry")

    val temp_table2 = spark.sql("SELECT CountryorRegion, sum(MaxConfirmed) as MaxConfirmed FROM MaxCasesByStateCountry GROUP BY CountryorRegion")
    temp_table2.createOrReplaceTempView("MaxCasesByCountry")
    println("Total cases worldwide")
    spark.sql("SELECT sum(MaxConfirmed) as WorldwideCases FROM MaxCasesByCountry").show
    println("The 5 countries with the most cases")
    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByCountry ORDER BY MaxConfirmed DESC LIMIT 5").show
    spark.close()
  }
}
