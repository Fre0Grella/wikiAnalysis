import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.util.Random


object wikipediaBusFactorAnalysis {
  private val p = new mediaWikiHistorySchema() // parser data schema

  def bus(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wikipedia Bus Factor")
    val sc   = new SparkContext(conf)
    println("======Starting Wikipedia Bus Factor Analysis Job======")

    sc.setLogLevel("WARN")


    Logger.getLogger("org.apache.spark.storage.MemoryStore").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)


    //println("Loading page to categories mapping...")
    val categories    = sc.textFile("output/page_to_root_categories/part-*")
    val articleTopics = categories
      .map(_.split("\t"))
      .map(data => (data(0).toLong, data(1).toLong))
      .persist(StorageLevel.MEMORY_AND_DISK)

    //printf("Total Articles with Categories: %d\n", articleTopics.count())

    println("Loading user contributions...")
    val historyDump       = sc.textFile("dataset/wikimedia_dumps/*.tsv.bz2")
   // println(s"Raw history lines: ${historyDump.count()}")
    val filteredInput = historyDump
      .map(_.split("\t",-1))
      .filter(filterEvent)
      .map(data =>
        (
          data(p.idx("page_id")).toLong,
          data(p.idx("page_title")),
          data(p.idx("event_user_text")),
          data(p.idx("revision_text_bytes_diff"))
        ))

      filteredInput
      .takeSample(withReplacement = false, 10, Random.nextLong())
      .foreach(s => printf("Sample Contribution: %s\n",s.toString() ))



    val userContributions = filteredInput
      .keyBy{case (page_id, _, _, _) => page_id}
      .join(articleTopics)
    val data              = userContributions.take(100)
    println("Sample Data:")
    data.foreach(println)
  }

  private def filterEvent(data: Array[String]): Boolean =
      data(p.idx("event_entity")) == "revision" &&      // KEEP revisions
      //data(p.idx("event_type")) == "create" &&          // KEEP creates
      //data(p.idx("page_namespace")) == "0" &&           // KEEP main namespace (articles)
      //data(p.idx("user_is_bot_by")).isEmpty &&
      data(p.idx("page_id")) != ""                   // KEEP rows with page_id


  def main(args: Array[String]): Unit = {
    bus(args)
  }
}

case class UserContribution(
  page_id: Int,
  page_title: String,
  event_user_text: String,
  revision_text_bytes_diff: Int
)
