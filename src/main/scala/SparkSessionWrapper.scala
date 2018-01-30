import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  def initSparkSession(appConfig: AppConfig): SparkSession = {
    val spark: SparkSession = {
      SparkSession
        .builder()
        .master(s"local[${appConfig.cores}]")
        .appName("Inclusion dependency discovery")
        .getOrCreate()
    }
    spark
  }

  def disableLogging(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }
}
