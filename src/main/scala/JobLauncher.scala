object JobLauncher {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }
    val deploymentMode: String = args(0)
    args(1) match {
      case "cat" => wikipediaCategoryAnalysis.main(Array(deploymentMode))
      case "bus" => wikipediaBusFactorAnalysis.main(Array(deploymentMode))
      case "all" =>
        wikipediaCategoryAnalysis.main(Array(deploymentMode))
        wikipediaBusFactorAnalysis.main(Array(deploymentMode))
      case _     => System.err.println("Usage: <root-graph|history> [options]")
    }
  }

}
