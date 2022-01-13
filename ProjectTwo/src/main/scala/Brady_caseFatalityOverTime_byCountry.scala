import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, format_number}

import scala.io.StdIn.readLine

object Brady_caseFatalityOverTime_byCountry {
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
//    println("completeData: raw data; no modifications; dates NOT formatted. Check that all dates of the month are present for all countries.")
//    spark.sql("SELECT * FROM completeData").show()

//    Formats date.
    val maxForEachCountryMonth = spark.sql( " SELECT " +
      "DATE_FORMAT(ObservationDate,\"yyyy-MM\") AS Month, " +
      "CountryOrRegion, " +
      "ProvinceOrState, " +
      "MAX(Confirmed) AS maxCases, " +
      "MAX(Deaths) AS maxDeaths " +
      "FROM completeData " +
      "GROUP BY 1,2,3").toDF()

    val str3 = "maxForEachCountryMonth: Formats date. For each month, and for each country within that month, " +
      "grabs the cumulative number of cases and deaths at the end of the month (the maximum for that month)."
    val maxForEachCountryMonth2 = maxForEachCountryMonth
      .withColumn("Month", date_format(col("Month"),"yyyy-MM"))
      .withColumn("CountryorRegion",col("CountryorRegion"))
      .withColumn("ProvinceOrState",col("ProvinceOrState"))
      .withColumn("maxCases",col("maxCases"))
      .withColumn("maxDeaths",col("maxDeaths"))
    maxForEachCountryMonth2.createTempView("maxForEachCountryMonth")
//    maxForEachCountryMonth2.printSchema()
    println(str3)
//    spark.sql("SELECT * FROM maxForEachCountryMonth").show()

    val str4 = "This outputs the cumulative total of cases and deaths worldwide for every month."
    spark.sql("SELECT Month, " +
      "CountryorRegion, " +
      "FLOOR(SUM(maxCases)) AS totCumCases, " +
      "FLOOR(SUM(maxDeaths)) AS totCumDeaths " +
      "FROM maxForEachCountryMonth GROUP BY Month, CountryorRegion ORDER BY CountryorRegion, Month")
      .createOrReplaceTempView("cumulativeCasesAndDeathsByMonth")
//    println(str4)
//    spark.sql("SELECT * FROM cumulativeCasesAndDeathsByMonth").show()

    val str5 = "This outputs the cumulative cases and deaths for the current month AND the previous month (using LAG)."
//    Took out GROUP BY from following statement: (GROUP BY Month, CountryorRegion)
//    Changed OVER(ORDER BY totCumCases ASC) to OVER(ORDER BY Month ASC)
    spark.sql("SELECT Month, CountryorRegion, totCumCases, IFNULL(LAG(totCumCases,1) OVER(ORDER BY CountryorRegion ASC),0) AS prevMonthCases, totCumDeaths, IFNULL(LAG(totCumDeaths,1) OVER(ORDER BY CountryorRegion ASC),0) AS prevMonthDeaths FROM cumulativeCasesAndDeathsByMonth ORDER BY CountryorRegion, Month").createOrReplaceTempView("monthComparisonsCasesDeaths")
    println(str5)
//    spark.sql("SELECT * FROM monthComparisonsCasesDeaths").show(1000)

    spark.sql("SELECT CountryorRegion, Month, totCumCases - prevMonthCases AS newCases, totCumDeaths - prevMonthDeaths AS newDeaths FROM monthComparisonsCasesDeaths").createOrReplaceTempView("newCasesDeaths")
    spark.sql("SELECT * FROM newCasesDeaths ORDER BY CountryorRegion, Month")

    val newCasesDeathsCFR = spark.sql("SELECT CountryorRegion, Month, newCases, newDeaths, newDeaths / newCases AS CFR FROM newCasesDeaths").toDF()
    val newCasesDeathsCFR2 = newCasesDeathsCFR
      .withColumn("newCases",col("NewCases"))
      .withColumn("newDeaths",col("newDeaths"))
      .withColumn("CFR",format_number(col("CFR"),4))
//      createOrReplaceTempView("newCasesDeathsCFR")
//    newCasesDeathsCFR2.show(4000)

    newCasesDeathsCFR2.coalesce(1).write.csv("Brady_caseFatalityOverTime_byCountry_newCasesDeathsCFR2.csv")

    spark.close()

  }
}
