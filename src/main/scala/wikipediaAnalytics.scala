import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Seq

object wikipediaAnalytics {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wikipedia Bus Factor")
    val sc   = new SparkContext(conf)

    val allData    = sc.textFile("dataset/wikimedia_dumps/2025-11.enwiki.*.tsv.bz2")
    val parsedData = allData.map(_.split("\t"))
    val data       = parsedData.take(100).toSeq
    println("Sample Data:")
    data.foreach(row => println(row.mkString("\t")))
  }
}
