import org.apache.spark.sql.SparkSession

object Brady_totalNewCasesByCountryByMonth {
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

//    spark.sql("SELECT FLOOR(sum(Confirmed)) as TotalConfirmed FROM TotalConfirmed")
//      .show()
//
////    Brady: Case-fatality ratio by country.
//    spark.sql("SELECT CountryorRegion, " +
//      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
//      "FROM TotalConfirmed GROUP BY CountryorRegion ORDER BY 2 DESC").show()


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
//
//    //    Brady: cumulative case-fatality ratio by country by month
//    spark.sql("SELECT CountryorRegion, date_format(ObservationDate,'M-y') AS Month," +
//      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
//      "FROM TotalConfirmed GROUP BY 1, 2 ORDER BY 1, 2").show()
//
////    Ben: Country with most cumulative cases
//    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
//    temp_table.registerTempTable("MaxCasesByStateCountry")
//    println("The country with most cases and how many cases")
//    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show

    spark.sql("DROP TABLE IF EXISTS completeData")
    spark.sql("CREATE TABLE IF NOT EXISTS completeData(SNo INT, ObservationDate DATE, ProvinceOrState STRING, CountryOrRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'covid_19_data_fixed.csv' OVERWRITE INTO TABLE completeData")

    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-y\") AS Month, CountryOrRegion, ProvinceOrState, max(Confirmed) AS MaxCases, max(Deaths) AS MaxDeaths, max(Recovered) AS MaxRecovered " +
      " FROM completeData GROUP BY 1,3,2").createTempView("monthlyData")

    spark.sql("SELECT * FROM monthlyData").show()

    spark.sql(" SELECT Month, CountryorRegion, ProvinceorState, FLOOR(sum(MaxCases)) AS CumulativeMonthlyCases, FLOOR(sum(MaxDeaths)) AS CumulativeMonthlyDeaths, FLOOR(sum(MaxRecovered)) AS CumulativeMonthlyRecovered FROM monthlyData GROUP BY Month, CountryorRegion, ProvinceorState ORDER BY CumulativeMonthlyCases ASC").createTempView("monthlyCumulative")

    spark.sql(" SELECT Month, CountryorRegion, ProvinceorState, CumulativeMonthlyCases, CumulativeMonthlyDeaths, CumulativeMonthlyRecovered," +
      " LAG(CumulativeMonthlyCases,1) OVER(ORDER BY CumulativeMonthlyCases ASC) AS PreviousMonthCases," +
      " LAG(CumulativeMonthlyDeaths,1) OVER(ORDER BY CumulativeMonthlyDeaths ASC) AS PreviousMonthDeaths," +
      " LAG(CumulativeMonthlyRecovered,1) OVER(ORDER BY CumulativeMonthlyRecovered ASC) AS PreviousMonthRecovered" +
      " FROM monthlyCumulative" +
      " ORDER BY CumulativeMonthlyCases ASC").createTempView("monthlyCumulative2")

    spark.sql(" SELECT Month, CountryorRegion, ProvinceorState, CumulativeMonthlyCases - IFNULL(PreviousMonthCases,0) AS NewCases, CumulativeMonthlyDeaths - IFNULL(PreviousMonthDeaths,0) AS NewDeaths, CumulativeMonthlyRecovered - IFNULL(PreviousMonthRecovered,0) AS NewRecovered FROM monthlyCumulative2").show()



    spark.close()

  }
}
