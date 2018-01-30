import java.io.File

case class AppConfig(
  // Number of cores to use for spark session
  cores: Int = 4,
  // List of CSV files to analyze for INDs
  files: List[File] = List(),
  // Whether to keep alive the spark-ui for debugging
  debug: Boolean = false,
  // Default dir
  defaultDirectory: String = "./TPCH"
)

object ArgParser {
  def parseConfig(args: Array[String]): AppConfig = {
    val parser = new scopt.OptionParser[AppConfig]("spark-uind") {
      opt[Int]("cores").action((cores, config) =>
        config.copy(cores = cores)
      )
      opt[Boolean]("debug").action((debug, config) =>
        config.copy(debug = debug)
      )
      opt[String]("path").action((path, config) => {
        val files = getFilesForDirectory(path)
        config.copy(files = files)
      })
      opt[Seq[String]]("paths").action((paths, config) => {
        val files = paths.toList.map(path => new File(path))
        config.copy(files = files)
      })
    }

    parser.parse(args, AppConfig()) match {
      case Some(config) =>
        if (config.files.isEmpty) {
          return config.copy(files = getFilesForDirectory(config.defaultDirectory))
        }
        config
      case None => throw new IllegalArgumentException("No commandline args given")
    }
  }

  private def getFilesForDirectory(directory: String): List[File] = {
    val parentDir = new File(directory)
    if (parentDir.exists && parentDir.isDirectory) {
      parentDir.listFiles.filter(_.isFile).toList
    } else {
      throw new IllegalArgumentException("%s is not a directory" format directory)
    }
  }
}
