object JobLauncher {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("USAGE: JobLauncher <deployment-mode> <cat|bus|all> <overwrite|skip>")
      return
    }
    val deploymentMode: String = args(0)
    val writeRule: String      =
      args(2) match {
        case "overwrite" => 0.toString
        case "skip"      => 1.toString
        case _           =>
          System.err.println("Bad write rule. Use 'overwrite' or 'skip'.")
          return
      }

    printf("Chosen configuration: deploymentMode=%s, writeRule=%s\n", deploymentMode, writeRule)

    args(1) match {
      case "cat" => wikipediaCategoryAnalysis.main(Array(deploymentMode, writeRule))
      case "bus" => wikipediaBusFactorAnalysis.main(Array(deploymentMode, writeRule))
      case "all" =>
        wikipediaCategoryAnalysis.main(Array(deploymentMode, writeRule))
        wikipediaBusFactorAnalysis.main(Array(deploymentMode, writeRule))
      case _     => System.err.println("Usage: <root-graph|history> [options]")
    }
  }

}
