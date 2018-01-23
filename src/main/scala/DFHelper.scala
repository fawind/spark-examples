import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

object DFHelper {
  def castCol(df: DataFrame, cn: String, tpe: DataType) : DataFrame = {
    df.withColumn(cn, df(cn).cast(tpe))
  }
}