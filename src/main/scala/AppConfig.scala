import java.io.File

case class AppConfig (cores: Int = 1, files: List[File] = List())

object ArgParser {
  def parseConifg(args: Array[String]): AppConfig = {
    val parser = new scopt.OptionParser[AppConfig]("spark-uind") {
      opt[Int]("cores").action((cores, config) => config.copy(cores = cores))
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
      case Some(config) => config
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
