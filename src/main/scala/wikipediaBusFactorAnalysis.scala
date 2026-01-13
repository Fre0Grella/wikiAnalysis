import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.util.{Random, Try}

object wikipediaBusFactorAnalysis {
  private val p = new mediaWikiHistorySchema() // parser data schema

  def bus(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Wikipedia Bus Factor")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    val sc   = new SparkContext(conf)
    println("======Starting Wikipedia Bus Factor Analysis Job======")

    sc.setLogLevel("WARN")

    Logger.getLogger("org.apache.spark.storage.MemoryStore").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    // println("Loading page to categories mapping...")
    val decoder = RootDecoder.fromTsv("output/root_category_indices.tsv")(sc)

    val categories    = sc.textFile("output/page_to_root_categories/part-*")
    val articleTopics = categories
      .map(_.split("\t"))
      .map(data => (data(0).toInt, data(1).toLong))

    // printf("Total Articles with Categories: %d\n", articleTopics.count())

    println("Loading user contributions...")
    val historyDump   = sc.textFile("dataset/wikimedia_dumps/*.tsv.bz2")
    // println(s"Raw history lines: ${historyDump.count()}")
    val filteredInput = historyDump
      .map(_.split("\t", -1))
      .filter(filterEvent)
      .map(data =>
        (
          data(p.idx("page_id")).toInt,
          (
            data(p.idx("page_title")),
            data(p.idx("event_user_text")),
            data(p.idx("revision_text_bytes_diff")).toLong.abs
          )
        )
      )
      .join(articleTopics)
      .map { case (pageId, ((title, user, bytesDiff), cats)) =>
        (pageId, title, user, bytesDiff.toInt, decoder.categoriesFromMask(cats))
      }

//    val totalContributions = filteredInput
//      .flatMap { case (pageId, title, user, bytesDiff, cats) =>
//        cats.map { cat =>
//          cat -> (pageId, title, user, bytesDiff)
//        }
//      }

    val contributionPerUserPerCategory = filteredInput
      .flatMap { case (_, _, user, bytesDiff, cats) => cats.map(cat => ((user, cat), bytesDiff)) }
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val contributionPerCategory = contributionPerUserPerCategory
      .map { case ((_, cat), bytesDiff) => (cat, bytesDiff) }
      .reduceByKey(_ + _)
      .collectAsMap()

    println("Sample User Contribution Per Category")
    contributionPerUserPerCategory
      .takeSample(withReplacement = false, 30, Random.nextLong())
      .foreach { case ((user, cat), v) =>
        printf("%s at:%s\t->\t%d bytes\n", user, decoder.idToString(cat), v)
      }

    println("Sample Category Total Contribution")
    contributionPerCategory
      .foreach { case (k, v) => printf("%s\t->\t%d bytes\n", decoder.idToString(k), v) }

  }

  private def filterEvent(data: Array[String]): Boolean = {
    val bitdiff = data(p.idx("revision_text_bytes_diff"))
    data(p.idx("event_entity")) == "revision" && // KEEP revisions
      //data(p.idx("event_type")) == "create" &&   // KEEP creates
      //data(p.idx("page_namespace")) == "0" &&    // KEEP main namespace (articles)
      //data(p.idx("user_is_bot_by")).isEmpty &&
      data(p.idx("page_id")) != "" &&
      Try(bitdiff.toLong).isSuccess
  }

  def main(args: Array[String]): Unit = bus(args)
}

case class UserContribution(
  page_id: Int,
  page_title: String,
  event_user_text: String,
  revision_text_bytes_diff: Int
)
