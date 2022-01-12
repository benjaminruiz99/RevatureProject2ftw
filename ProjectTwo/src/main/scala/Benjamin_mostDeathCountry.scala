import org.apache.spark.sql.SparkSession


object Benjamin_mostDeathCountry {
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

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Deaths)) as MaxDeaths FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.registerTempTable("MaxDeathsByStateCountry")

    val temp_table2 = spark.sql("SELECT CountryorRegion, sum(MaxDeaths) as MaxDeaths FROM MaxDeathsByStateCountry GROUP BY CountryorRegion")
    temp_table2.createOrReplaceTempView("MaxDeathsByCountry")
    println("Total deaths worldwide")
    spark.sql("SELECT sum(MaxDeaths) AS WorldwideDeaths FROM MaxDeathsByCountry").show
    println("The 5 countries with the most deaths")
    spark.sql("SELECT CountryorRegion, MaxDeaths FROM MaxDeathsByCountry ORDER BY MaxDeaths DESC LIMIT 5").show
    spark.close()
  }
}
