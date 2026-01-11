import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.util.Random


object wikipediaBusFactorAnalysis {
  private val p = new mediaWikiHistorySchema() // parser data schema

  def bus(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wikipedia Bus Factor")
    val sc   = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("Loading page to categories mapping...")
    val categories    = sc.textFile("output/page_to_root_categories/part-*")
    val articleTopics = categories
      .map(_.split("\t"))
      .map(data => (data(0).toInt, data(1).toLong))
      .persist(StorageLevel.MEMORY_AND_DISK)

    printf("Total Articles with Categories: %d\n", articleTopics.count())

    println("Loading user contributions...")
    val historyDump       = sc.textFile("dataset/wikimedia_dumps/*.tsv.bz2")
    val filteredInput = historyDump
      .map(_.split("\t"))
      .filter(filterEvent)
      .persist(StorageLevel.MEMORY_AND_DISK)

    filteredInput
      .map(data =>
        (
          data(p.idx("page_id")),
          data(p.idx("page_title")),
          data(p.idx("event_user_text")),
          data(p.idx("revision_text_bytes_diff"))
        ))
      .takeSample(withReplacement = false, 100, Random.nextLong())
      .foreach(s => printf("Sample Contribution: %s\n",s.toString() ))



    val userContributions = filteredInput.map(data =>
        (
          data(p.idx("page_id")).toInt,
          data(p.idx("page_title")),
          data(p.idx("event_user_text")),
          data(p.idx("revision_text_bytes_diff"))
        )
      )
      .keyBy{case (page_id, _, _, _) => page_id}
      .join(articleTopics)
    val data              = userContributions.take(100)
    println("Sample Data:")
    data.foreach(println)
  }

  private def filterEvent(data: Array[String]): Boolean =
    data(p.idx("event_entity")) != "revision" &&
      data(p.idx("event_type")) != "create" &&
      data(p.idx("page_namespace")) != "0" &&
      data(p.idx("event_user_is_bot_by")) != ""

  def main(args: Array[String]): Unit = {
    println("======Starting Wikipedia Bus Factor Analysis Job======")
    bus(args)
  }
}

case class UserContribution(
  page_id: Int,
  page_title: String,
  event_user_text: String,
  revision_text_bytes_diff: Int
)
