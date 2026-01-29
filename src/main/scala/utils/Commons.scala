package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.InputStream

object Commons {

  @SuppressWarnings(Array("org.wartremover.warts.PlatformDefault"))
  def initializeSparkContext(deploymentMode: String, sc: SparkContext): Unit =
    if (deploymentMode == "remote") {
      val stream: InputStream = getClass.getResourceAsStream(Config.credentialsPath)
      val lines               = scala.io.Source.fromInputStream(stream).getLines.toList

      sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
      sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines(0))
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))
    }

  def getDatasetPath(deploymentMode: String, localPath: String, remotePath: String): String =
    if (deploymentMode == "local") {
      "file://" + Config.projectDir + "/" + localPath
    } else {
      "s3a://" + Config.s3BucketName + "/" + remotePath
    }

  def getDatasetPath(deploymentMode: String, path: String): String = getDatasetPath(
    deploymentMode,
    path,
    path
  )

}
