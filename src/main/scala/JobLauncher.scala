object JobLauncher {
  def main(args: Array[String]): Unit =
    args.headOption match {
      case Some("cat") => wikipediaCategoryAnalysis.main(args.tail)
      case Some("bus") => wikipediaBusFactorAnalysis.main(args.tail)
      case Some("all") =>
        wikipediaCategoryAnalysis.main(args.tail)
        wikipediaBusFactorAnalysis.main(args.tail)
      case _           => System.err.println("Usage: <root-graph|history> [options]")
    }
}
