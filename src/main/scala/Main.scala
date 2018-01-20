import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.trim

object Main extends App {

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val appConfig: AppConfig = ArgParser.parseConifg(args)
    println(appConfig)


    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._

    val tables: List[DataFrame] = appConfig.files.map(file => {
      var table = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)
      table.columns.foreach(column => {
        table = table.withColumn(column, trim(table.col(column)))
      })
      table
    })

    val df = tables.head

    val columns = df.columns
    val values = df.flatMap(_.getValuesMap[String](columns))

    values.head(1)
    println("DEBUGGER")
  }
}
