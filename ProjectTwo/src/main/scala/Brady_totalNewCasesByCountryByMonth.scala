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

//    spark.sql("DROP TABLE IF EXISTS TotalConfirmed")
//    spark.sql("CREATE TABLE TotalConfirmed(" +
//      "SNo INT," +
//      "ObservationDate DATE," +
//      "ProvinceorState STRING," +
//      "CountryorRegion STRING," +
//      "LastUpdate STRING," +
//      "Confirmed DOUBLE," +
//      "Deaths DOUBLE," +
//      "Recovered DOUBLE)" +
//      "row format delimited fields terminated by ','" +
//      "stored as textfile"
//    )
//
//    spark.sql(
//      "LOAD DATA LOCAL INPATH 'covid_19_data_fixed.csv'" +
//        "OVERWRITE INTO TABLE TotalConfirmed"
//    )

    spark.sql("DROP TABLE IF EXISTS completeData")
        spark.sql("CREATE TABLE IF NOT EXISTS completeData(SNo INT, ObservationDate DATE, ProvinceOrState STRING, CountryOrRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
        spark.sql("LOAD DATA LOCAL INPATH 'covid_19_data_fixed.csv' OVERWRITE INTO TABLE completeData")

    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-y\") AS Month, CountryOrRegion, ProvinceOrState, max(Confirmed) AS MaxCases, max(Deaths) AS MaxDeaths, max(Recovered) AS MaxRecovered " +
      " FROM completeData GROUP BY 1,3,2").createTempView("monthlyData")

    spark.sql("SELECT * FROM monthlyData").show()

    spark.sql(" SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "FLOOR(sum(MaxCases)) AS CumulativeMonthlyCases, " +
      "FLOOR(sum(MaxDeaths)) AS CumulativeMonthlyDeaths, " +
      "FLOOR(sum(MaxRecovered)) AS CumulativeMonthlyRecovered " +
      "FROM monthlyData " +
      "GROUP BY Month, CountryorRegion, ProvinceorState")
//      "ORDER BY CumulativeMonthlyCases ASC"
//    )
      .createTempView("monthlyCumulative")

    spark.sql(" SELECT Month, CountryorRegion, ProvinceorState, CumulativeMonthlyCases, CumulativeMonthlyDeaths, CumulativeMonthlyRecovered, " + " LAG(CumulativeMonthlyCases,1) OVER(ORDER BY CumulativeMonthlyCases ASC) AS PreviousMonthCases, " + " LAG(CumulativeMonthlyDeaths,1) OVER(ORDER BY CumulativeMonthlyDeaths ASC) AS PreviousMonthDeaths, " + " LAG(CumulativeMonthlyRecovered,1) OVER(ORDER BY CumulativeMonthlyRecovered ASC) AS PreviousMonthRecovered " + " FROM monthlyCumulative").createTempView("monthlyCumulative2")
//    " ORDER BY Month, CountryorRegion,ProvinceorState"

    spark.sql(" SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "CumulativeMonthlyCases - IFNULL(PreviousMonthCases,0) AS NewCases, " +
      "CumulativeMonthlyDeaths - IFNULL(PreviousMonthDeaths,0) AS NewDeaths, " +
      "CumulativeMonthlyRecovered - IFNULL(PreviousMonthRecovered,0) AS NewRecovered " +
      "FROM monthlyCumulative2")
      .createOrReplaceTempView("monthlyCumulative3")

    spark.sql("SELECT Month, CountryorRegion, ProvinceorState, NewCases, NewDeaths, NewRecovered, NewDeaths / NewCases AS monthlyCaseFatalityRatio FROM monthlyCumulative3 ORDER BY Month, CountryorRegion,ProvinceorState").createOrReplaceTempView("monthlyCumulative4")
    spark.sql("SELECT Month, CountryorRegion, ProvinceorState, NewCases, NewDeaths, NewRecovered, monthlyCaseFatalityRatio FROM monthlyCumulative4 WHERE monthlyCaseFatalityRatio < 1 ORDER BY monthlyCaseFatalityRatio DESC").show()
//    WHERE monthlyCaseFatalityRatio < 1
    spark.close()

  }
}
