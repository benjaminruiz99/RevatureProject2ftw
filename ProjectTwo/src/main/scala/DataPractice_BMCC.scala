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
    //val data_path = "C:\\Users\\benja\\Desktop\\Revature\\RevatureProject2ftw\\ProjectTwo\\Data\\covid_19_data_complete(Kaggle).csv"
    //val load_data = "LOAD DATA LOCAL INPATH " + data_path + " OVERWRITE INTO TABLE TotalConfirmed"

    //spark.sql("DROP TABLE IF EXISTS TotalConfirmed")
    //spark.sql("Drop table if exists TotalConfirmed")
    spark.sql("CREATE TABLE IF NOT EXISTS TotalConfirmed(SNo INT, ObservationDate STRING, ProvinceorState STRING, CountryorRegion STRING, LastUpdate STRING, Confirmed DOUBLE, Deaths DOUBLE, Recovered DOUBLE) row format delimited fields terminated by ',' stored as textfile")
    spark.sql(s"LOAD DATA LOCAL INPATH \'covid_19_data_complete(Kaggle).csv\' OVERWRITE INTO TABLE TotalConfirmed")
    //                                            C:\Users\benja\Desktop\Revature\RevatureProject2ftw\csv data\KaggleData(Complete)
    //spark.sql(load_data)

    //var confirmed_csv = spark.read.csv("C:\\Users\\benja\\Desktop\\Revature\\RevatureProject2ftw\\csv data\\KaggleData(Complete)\\covid_19_data_complete(Kaggle).csv")
    //spark.sql("drop table if exists TotalConfirmedCSV")
    //confirmed_csv.write.saveAsTable("TotalConfirmedCSV")
    //spark.sql("select * from TotalConfirmedCSV").show
    //spark.sql("select Distinct(ProvinceorState) from TotalConfirmed ORDER BY ProvinceorState").show
    //spark.sql("select * from TotalConfirmed WHERE SNo=563").show
    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    //temp_table.show
    temp_table.registerTempTable("MaxCasesByStateCountry")
    //spark.sql("select sum(MaxConfirmed) from MaxCasesByStateCountry").show
    println("The country with most cases and how many cases")
    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show
    spark.close()
  }


}
