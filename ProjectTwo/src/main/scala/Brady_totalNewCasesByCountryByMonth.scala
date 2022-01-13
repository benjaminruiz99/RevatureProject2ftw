import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine

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
// Raw data. No modifications.
    spark.sql("DROP TABLE IF EXISTS completeData")
        spark.sql("CREATE TABLE IF NOT EXISTS completeData" +
          "(SNo INT, " +
          "ObservationDate DATE, " +
          "ProvinceOrState STRING, " +
          "CountryOrRegion STRING, " +
          "LastUpdate STRING, " +
          "Confirmed DOUBLE, " +
          "Deaths DOUBLE, " +
          "Recovered DOUBLE) " +
          "row format delimited fields terminated by ',' " +
          "stored as textfile")
        spark.sql("LOAD DATA LOCAL INPATH 'covid_19_data_fixed.csv' OVERWRITE INTO TABLE completeData")
    println("completeData: raw data; no modifications; dates NOT formatted. Check that all dates of the month are present for all countries.")
    spark.sql("SELECT * FROM completeData").show()

//    Formats date.
    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-y\") AS Month, " +
      "CountryOrRegion, " +
      "ProvinceOrState, " +
      "max(Confirmed) AS MaxCases, " +
      "max(Deaths) AS MaxDeaths, " +
      "max(Recovered) AS MaxRecovered " +
      " FROM completeData " +
      "GROUP BY 2,3,1")
      .createTempView("monthlyData")
    println("monthlyData: Dates now formatted. " +
      "I think you really need to group by countries/provinces/states first." +
      "You also need to group by date; if you don't, this query will calculate " +
      "the maximums for each country across the entire time span of the data!" +
      "From what I can see, the data matches up: When I look for the maximum for" +
      " a given country during a given month, it matches the data shown in this table.")
    spark.sql("SELECT * FROM monthlyData").show()

// monthlyCumulative. grouped by country, state/prov, month
    spark.sql(" SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "FLOOR(sum(MaxCases)) AS CumulativeMonthlyCases, " +
      "FLOOR(sum(MaxDeaths)) AS CumulativeMonthlyDeaths, " +
      "FLOOR(sum(MaxRecovered)) AS CumulativeMonthlyRecovered " +
      "FROM monthlyData " +
      "GROUP BY 2, 3, 1")
//      "ORDER BY CumulativeMonthlyCases ASC"
//    )
      .createTempView("monthlyCumulative")
    println("monthlyCumulative. grouped by country, state/prov, month")
    spark.sql("SELECT * FROM monthlyCumulative").show()


    spark.sql(" SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "CumulativeMonthlyCases, " +
      "LAG(CumulativeMonthlyCases,1) OVER(ORDER BY CumulativeMonthlyCases ASC) AS PreviousMonthCases, " +
      "CumulativeMonthlyDeaths, " +
      "LAG(CumulativeMonthlyDeaths,1) OVER(ORDER BY CumulativeMonthlyDeaths ASC) AS PreviousMonthDeaths, " +
      "CumulativeMonthlyRecovered, " +
      "LAG(CumulativeMonthlyRecovered,1) OVER(ORDER BY CumulativeMonthlyRecovered ASC) AS PreviousMonthRecovered " +
      "FROM monthlyCumulative")
      .createTempView("lastMonthLag")
//    " ORDER BY Month, CountryorRegion,ProvinceorState"
    println("lastMonthLag: Here's where we see this " +
      "month's and last month's cumulative numbers side-by-side")
    spark.sql("SELECT * FROM lastMonthLag").show()

    spark.sql(" SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "CumulativeMonthlyCases - IFNULL(PreviousMonthCases,0) AS NewCases, " +
      "CumulativeMonthlyDeaths - IFNULL(PreviousMonthDeaths,0) AS NewDeaths, " +
      "CumulativeMonthlyRecovered - IFNULL(PreviousMonthRecovered,0) AS NewRecovered " +
      "FROM lastMonthLag")
      .createOrReplaceTempView("monthlyCumulative3")

    println("monthlyCumulative3: this is where the subtraction happens, " +
      "where we first see the number of NEW monthly cases/deaths/recoveries")
    spark.sql("SELECT * FROM monthlyCumulative3 " +
      "WHERE CountryorRegion = 'US' AND NewCases > 50 " +
      "ORDER BY ProvinceorState, Month").show(500)

    spark.sql("SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "NewCases, " +
      "NewDeaths, " +
      "NewRecovered, " +
      "NewDeaths / NewCases AS monthlyCaseFatalityRatio " +
      "FROM monthlyCumulative3")
      .createOrReplaceTempView("monthlyCumulative4")
    spark.sql("SELECT Month, " +
      "CountryorRegion, " +
      "ProvinceorState, " +
      "NewCases, " +
      "NewDeaths, " +
      "NewRecovered, " +
      "monthlyCaseFatalityRatio " +
      "FROM monthlyCumulative4 " +
      "WHERE CountryorRegion = 'US'" +
      "ORDER BY ProvinceorState, Month").show(500)
//    WHERE monthlyCaseFatalityRatio < 1
    spark.close()

  }
}
