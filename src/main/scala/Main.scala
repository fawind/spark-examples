import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {

  override def main(args: Array[String]): Unit = {
    val appConfig: AppConfig = ArgParser.parseConifg(args)
    println(appConfig)
  }


  def getFilesForDirectory(directory: String): List[File] = {
    val parentDir = new File(directory)
    if (parentDir.exists && parentDir.isDirectory) {
        parentDir.listFiles.filter(_.isFile).toList
    } else {
      throw new IllegalArgumentException("%s is not a directory" format directory)
    }
  }
}
