package utils

// src/main/scala/Config.scala
object Config {
  val s3BucketName = "wiki-analysis-galeri"
  val projectDir   =
    "C:\\Users\\marco\\Drawer2\\2-Laurea-Magistrale\\2Anno\\BigData\\Project\\wikiAnalysis" // Path locale

  // Path S3
  val s3DatasetPath = s"datasets/"
  val s3OutputPath  = s"output/"
  val s3HistoryPath = s"spark-logs/"

  val credentialsPath: String = "/aws_credentials"

}
