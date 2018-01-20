case class AppConfig (path: String = "", cores: Int = 1)

object ArgParser {
  def parseConifg(args: Array[String]): AppConfig = {
    val parser = new scopt.OptionParser[AppConfig]("spark-uind") {
      opt[Int]("cores").action((cores, config) => config.copy(cores = cores))
      opt[String]("path").action((path, config) => config.copy(path = path))
    }
    parser.parse(args, AppConfig()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException("No commandline args given")
    }
  }
}
