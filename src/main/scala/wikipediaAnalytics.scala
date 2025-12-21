import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Seq
object wikipediaAnalytics {
  private val p = new mediaWikiHistorySchema() //parser data schema


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wikipedia Bus Factor")
    val sc   = new SparkContext(conf)

    val categories = sc.textFile("dataset/categories/articles_main_topics.tsv")
    val articleTopics = categories.map(_.split("\t"))
      .map(data => (data(0).toInt, data(1)))

    val historyDump    = sc.textFile("dataset/wikimedia_dumps/2025-11.enwiki.*.tsv.bz2")
    val userContributions = historyDump.map(_.split("\t"))
      .filter(filterEvent)
      .map(data => UserContribution(
        data(p.idx("page_id")).toInt,
        data(p.idx("page_title")),
        data(p.idx("event_user_text")),
        data(p.idx("revision_text_bytes_diff")).toInt)
      ).keyBy(_.page_id).join(articleTopics)
      .map(data =>)
    val data = userContributions.take(100)
    println("Sample Data:")
    data.foreach(println)
  }

  private def filterEvent(data: Array[String]): Boolean = {
    data(p.idx("event_entity")) != "revision" &&
      data(p.idx("event_type")) != "create"  &&
      data(p.idx("page_namespace")) != "0" &&
      data(p.idx("event_user_is_bot_by")) != ""
  }
}

case class UserContribution (
                              page_id: Int,
                              page_title: String,
                              event_user_text: String,
                              revision_text_bytes_diff: Int
                            )
