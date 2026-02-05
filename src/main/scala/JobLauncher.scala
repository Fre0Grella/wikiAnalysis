object JobLauncher {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println(
        "USAGE: JobLauncher <deployment-mode> <cat|bus|all> <overwrite|skip> <baseline|optimized>"
      )
      println("  deployment-mode: local or remote")
      println("  job: cat (category analysis), bus (bus factor)")
      println("  write-rule: overwrite or skip")
      println("  version: baseline (non-optimized) or optimized")
      return
    }

    val deploymentMode: String = args(0)
    val jobType: String        = args(1)
    val writeRule: String      =
      args(2) match {
        case "overwrite" => 0.toString
        case "skip"      => 1.toString
        case _           =>
          System.err.println("Bad write rule. Use 'overwrite' or 'skip'.")
          return
      }
    val version: String        = args(3)

    printf(
      "Chosen configuration: deploymentMode=%s, jobType=%s, writeRule=%s, version=%s\n",
      deploymentMode,
      jobType,
      writeRule,
      version
    )

    // Validate version parameter
    if (version != "baseline" && version != "optimized") {
      System.err.println("Invalid version. Use 'baseline' or 'optimized'.")
      return
    }

    // Route to appropriate version based on parameters
    (jobType, version) match {
      case ("cat", "baseline") =>
        NonOptimized_wikipediaCategoryAnalysis.main(Array(deploymentMode, writeRule))

      case ("cat", "optimized") => wikipediaCategoryAnalysis.main(Array(deploymentMode, writeRule))

      case ("bus", "baseline") =>
        NonOptimized_wikipediaBusFactorAnalysis.main(Array(deploymentMode, writeRule))

      case ("bus", "optimized") => wikipediaBusFactorAnalysis.main(Array(deploymentMode, writeRule))

      case _ => System.err.println("Invalid job type. Use 'cat', 'bus', or 'all'.")
    }
  }
}
