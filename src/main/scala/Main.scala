import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Main extends App with SparkSessionWrapper {

  override def main(args: Array[String]): Unit = {
    val appConfig: AppConfig = ArgParser.parseConfig(args)
    println(appConfig)
    disableLogging()

    val spark = initSparkSession(appConfig)
    import spark.implicits._

    // Read in dataframes from CSV files
    val tables: List[DataFrame] = appConfig.files.map(file => {
      val table = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(file.getAbsolutePath)

      val df = table.columns
        .foldLeft(table) {(df, colName) =>
          df.withColumn(colName, trim(df.col(colName)))
        }
      df
    })

    // Pull out (attribute, value) cells from all table relations
    val tableCells = tables.map(df => {
      val columns = df.columns
      val cells = df
        .flatMap(_.getValuesMap[String](columns))
        .withColumnRenamed("_1", "attribute")
        .withColumnRenamed("_2", "value")
      cells
    })
    val allCells = tableCells.reduce(_ union _)

    // Collect attribute sets from identical values
    val attributeSets = allCells
      .groupBy("value")
      .agg(collect_set("attribute").as("attributes"))
      .drop("value")

    // Build inclusion lists by filtering for the exploded element
    def filterAttr(attr: String, attrSet: Seq[String]) : Seq[String] = {
      // Note: UDFs aren't properly optimized, is there another way to do this?
      attrSet.filter(str => str != attr)
    }
    val createInclusionList = udf(filterAttr(_: String, _: Seq[String]))
    val inclusions = attributeSets
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
      .withColumnRenamed("ReduceAggregator(scala.collection.Seq)", "referenced")
      .withColumnRenamed("value", "dependent")

    // Collect and print INDs
    val inds = intersections
      .filter(size(col("referenced")) > 0)
      .collect()

    inds.foreach(row => {
      val dependent = row.getAs[String]("dependent")
      val references = row.getAs[Seq[String]]("referenced")
      println(dependent + " < " + references.mkString(","))
    })

    // Keep spark-ui alive for debugging
    if (appConfig.ui) {
      System.in.read
      spark.stop()
    }
  }
}
