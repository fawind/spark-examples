object Main extends App {

  override def main(args: Array[String]): Unit = {
    val appConfig: AppConfig = ArgParser.parseConifg(args)
    println(appConfig)
  }
}
