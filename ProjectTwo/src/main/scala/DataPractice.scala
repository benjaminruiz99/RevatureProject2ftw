import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._

import sys.process._

object DataPractice {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sparkContext.setLogLevel("WARN")

    //spark.sql("CREATE TABLE IF NOT EXISTS POTDPartyData(ID INT, Rank INT, Name STRING, Server STRING, Datacenter STRING, Job STRING, Score INT, Floor INT, Date STRING) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'POTDParty.txt' INTO TABLE POTDPartyData")

    val ConfirmedWorldWideDF = spark.read.options(Map("header" -> "true"))
      .csv("time_series_covid_19_confirmed.csv")
    ConfirmedWorldWideDF.show()



    spark.close()
  }
}
