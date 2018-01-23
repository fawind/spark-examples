import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, ArrayType}
import DFHelper._


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
    val cells = df.flatMap(_.getValuesMap[String](columns))

    val attributeSet = cells
      .groupBy("_2")
      .agg(collect_set("_1").as("attributes"))
      .drop("_2")

    // Build inclusion lists by filtering for the exploded element
    def filterAttr(attr: String, attrSet: Seq[String]) : Seq[String] = {
      // Note: UDFs aren't properly optimized, is there another way to do this?
      attrSet.filter(str => str != attr)
    }
    val createInclusionList = udf(filterAttr(_: String, _: Seq[String]))
    val inclusions = attributeSet
      .withColumn("attribute", explode(col("attributes")))
      .withColumn("inclusionList", createInclusionList($"attribute", $"attributes"))
      .drop("attributes")

    // Intersect inclusion lists
    // TODO: discard groups containing empty sets
    val intersections = inclusions.map {
      case Row(attr: String, inclusionList: Seq[String]) => (attr, inclusionList)
    }.groupByKey(row => row._1)
      .mapValues(row => row._2)
      .reduceGroups((acc, s) => acc.toSet.intersect(s.toSet).toSeq)
      .withColumnRenamed("ReduceAggregator(scala.collection.Seq)", "indSet")
      .withColumnRenamed("value", "dependent")

    /*
    implicit def immutableSetEncoder[T]: Encoder[Set[T]] = Encoders.kryo[scala.collection.immutable.Set[T]]

    val intersections = inclusions.map {
      case Row(attr: String, inclusionList: Seq[String]) => (attr, inclusionList)
    }.groupByKey(row => row._1)
      .mapValues(row => row._2.toSet)
      .reduceGroups((acc, s) => acc.intersect(s))
      .withColumnRenamed("ReduceAggregator(scala.collection.immutable.Set)", "indSet")
      .withColumnRenamed("value", "dependent")

    val intersections2 = inclusions
    .groupByKey(row => row.getAs[String]("attribute"))
    .mapValues(row => row.getAs[Seq[String]]("inclusionList").toSet)
    .reduceGroups((acc, s) => acc.intersect(s))
    */

    // Collect and print INDs
    val inds = intersections
      .filter(size(col("indSet")) > 0)
      .collect()

    inds.foreach(row => {
      val dependent = row.getAs[String]("dependent")
      val references = row.getAs[Seq[String]]("indSet")
      println(dependent + " < " + references.mkString(","))
    })

    // Keep Spark-UI alive
    System.in.read
    spark.stop()
  }
}
