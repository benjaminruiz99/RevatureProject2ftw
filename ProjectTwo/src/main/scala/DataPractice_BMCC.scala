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
    spark.sql(s"LOAD DATA LOCAL INPATH 'covid_data_fixed.csv' OVERWRITE INTO TABLE TotalConfirmed")

    val temp_table = spark.sql("SELECT ObservationDate,ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion,ObservationDate")
    temp_table.registerTempTable("MaxCasesByStateCountry")

    val temp_table2 = spark.sql("SELECT ObservationDate, CountryorRegion, sum(MaxConfirmed) as MaxConfirmed FROM MaxCasesByStateCountry GROUP BY ObservationDate,CountryorRegion Order By ObservationDate DESC")
    temp_table2.createOrReplaceTempView("MaxCasesByCountry")
    println("Total cases worldwide")
    spark.sql("SELECT sum(MaxConfirmed) as WorldwideCases FROM MaxCasesByCountry GROUP BY ObservationDate LIMIT 1").show
    println("The 20 countries with the most cases")
    spark.sql("SELECT CountryorRegion, first(MaxConfirmed) as Cases FROM MaxCasesByCountry GROUP BY CountryorRegion ORDER BY 2 DESC").show
    spark.close()
  }
}
