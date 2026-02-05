package utils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

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
      Config.projectDir + "/" + localPath
    } else {
      "s3a://" + Config.s3BucketName + "/" + remotePath
    }

  def getDatasetPath(deploymentMode: String, path: String): String = getDatasetPath(
    deploymentMode,
    path,
    path
  )

  /** Check if a path exists (works with both local filesystem and S3)
    * @param sc
    *   SparkContext
    * @param path
    *   Full path to check (e.g., "s3a://bucket/path" or "file:///local/path")
    * @return
    *   true if path exists, false otherwise
    */
  def exists(sc: SparkContext, path: String): Boolean =
    try {
      val hadoopPath = new Path(path)
      val fs         = hadoopPath.getFileSystem(sc.hadoopConfiguration)
      fs.exists(hadoopPath)
    } catch {
      case e: Exception =>
        println(s"Error checking existence of $path: ${e.getMessage}")
        false
    }

  /** Delete path if it exists (recursive)
    * @param sc
    *   SparkContext
    * @param path
    *   Full path to delete
    * @return
    *   true if path was deleted, false if it didn't exist or error occurred
    */
  def deleteIfExists(sc: SparkContext, path: String): Boolean =
    try {
      val hadoopPath = new Path(path)
      val fs         = hadoopPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(hadoopPath)) {
        println(s"Deleting existing path: $path")
        fs.delete(hadoopPath, true) // true = recursive delete
        true
      } else {
        false
      }
    } catch {
      case e: Exception =>
        println(s"Error deleting $path: ${e.getMessage}")
        false
    }

  /** Move/rename a file or directory
    * @param sc
    *   SparkContext
    * @param srcPath
    *   Source path
    * @param dstPath
    *   Destination path
    * @return
    *   true if successfully moved, false otherwise
    */
  def move(sc: SparkContext, srcPath: String, dstPath: String): Boolean =
    try {
      val hadoopSrc = new Path(srcPath)
      val hadoopDst = new Path(dstPath)
      val fs        = hadoopSrc.getFileSystem(sc.hadoopConfiguration)

      if (!fs.exists(hadoopSrc)) {
        println(s"Source path does not exist: $srcPath")
        false
      } else {
        println(s"Moving $srcPath -> $dstPath")
        fs.rename(hadoopSrc, hadoopDst)
      }
    } catch {
      case e: Exception =>
        println(s"Error moving $srcPath to $dstPath: ${e.getMessage}")
        false
    }

  /** Rename a file or directory (same as move but clearer intent)
    * @param sc
    *   SparkContext
    * @param oldPath
    *   Current path
    * @param newPath
    *   New path
    * @return
    *   true if successfully renamed, false otherwise
    */
  def rename(sc: SparkContext, oldPath: String, newPath: String): Boolean = move(
    sc,
    oldPath,
    newPath
  )

}
