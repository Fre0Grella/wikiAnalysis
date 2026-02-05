import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkConf, SparkContext }
import utils.Commons

import scala.collection.mutable
import scala.util.Try

object wikipediaBusFactorAnalysis {
  private val p = new mediaWikiHistorySchema() // parser data schema

  @SuppressWarnings(
    Array("org.wartremover.warts.While", "org.wartremover.warts.MutableDataStructures")
  )
  def main(args: Array[String]): Unit = {

    val topBound = 2000

    implicit val deploymentMode: String =
      if (args.length > 0)
        args(0)
      else
        "local"

    implicit val writeRule: Int =
      if (args.length > 1)
        args(1).toInt
      else
        1

    val conf = new SparkConf()
      .setAppName("Wikipedia Bus Factor")

    val sc = new SparkContext(conf)
    Commons.initializeSparkContext(deploymentMode, sc)
    println("======Starting Wikipedia Bus Factor Analysis Job======")

    sc.setLogLevel("WARN")

    Logger.getLogger("org.apache.spark.storage.MemoryStore").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    // println("Loading page to categories mapping...")
    val decoder = RootDecoder.fromTsv("output/root_category_indices.tsv")(sc, deploymentMode)

    val categories    = sc.textFile(
      Commons.getDatasetPath(deploymentMode, "output/page_to_root_categories/part-*")
    )
    val articleTopics = categories
      .map(_.split("\t"))
      .map(data => (data(0).toInt, data(1).toLong))

    // printf("Total Articles with Categories: %d\n", articleTopics.count())

    val historyDump   = sc.textFile(
      Commons.getDatasetPath(deploymentMode, "dataset/wikimedia_dumps/*.tsv.bz2")
    )
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

    val contributionPerUserPerCategory = filteredInput
      .flatMap { case (_, _, user, bytesDiff, cats) => cats.map(cat => ((user, cat), bytesDiff)) }
      .reduceByKey(_ + _)

    val busFactor = contributionPerUserPerCategory
      .map { case ((user, cat), bytes) => (cat, (user, bytes)) }
      .aggregateByKey(
        (
          0L,
          mutable
            .PriorityQueue
            .empty[(String, Long)](Ordering.by[(String, Long), Long](_._2).reverse)
        )
      )(
        { case ((totalBytes, queue), (user, bytes)) =>
          queue += ((user, bytes))
          if (queue.size > topBound)
            queue.dequeue() // Keep top 100
          (totalBytes + bytes, queue)
        },
        { case ((total1, queue1), (total2, queue2)) =>
          queue1 ++= queue2
          while (queue1.size > topBound)
            queue1.dequeue()
          (total1 + total2, queue1)
        }
      )
      .mapValues { case (totalBytes, queue) =>
        val threshold = totalBytes * 0.5
        val sorted    = queue.toArray.sortBy(-_._2)

        val busFactor =
          sorted
            .scanLeft(0L)(_ + _._2)
            .tail
            .indexWhere(_ >= threshold) match {
            case -1  => sorted.length // If threshold never reached, all contributors needed
            case idx => idx + 1       // +1 because we want the count, not the index
          }

        (busFactor, totalBytes, sorted.take(busFactor))
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    saveOutputs(
      busFactor,
      "output",
      sc
    )
    busFactor.unpersist()
    println("Outputs saved to output/ directory.")
    println("======Wikipedia Bus Factor Analysis Job Completed======")
    sc.stop()
  }

  private def filterEvent(data: Array[String]): Boolean = {
    val bitdiff = data(p.idx("revision_text_bytes_diff"))
    data(p.idx("event_entity")) == "revision" &&       // KEEP revisions
    data(p.idx("user_is_bot_by")).isEmpty &&           // FILTER out known bots
    !data(p.idx("event_user_text")).contains("bot") && // FILTER out probable bots
    !data(p.idx("event_user_text")).contains("Bot") &&
    !data(p.idx("event_user_text")).contains("BOT") &&
    data(p.idx("page_id")) != "" &&
    Try(bitdiff.toLong).isSuccess
  }

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def saveOutputs(
    result: RDD[(Int, (Int, Long, Array[(String, Long)]))],
    dirPath: String,
    sc: SparkContext
  )(implicit deploymentMode: String, writeRule: Int): Unit = {
    val busFactorPath      = Commons.getDatasetPath(deploymentMode, dirPath + "/bus_factor")
    val topContributorPath = Commons.getDatasetPath(deploymentMode, dirPath + "/top_contributors")
    val busFactorFile      = Commons.getDatasetPath(deploymentMode, "output/bus_factor.tsv")
    val topContribFile     = Commons.getDatasetPath(deploymentMode, "output/top_contributors.tsv")

    if (
      writeRule == 1 && Commons.exists(sc, busFactorFile) && Commons.exists(
        sc,
        topContribFile
      )
    ) {
      println(s"Output already exists at $dirPath. Skipping write as per write rule.")
      return
    }

    val decoder = RootDecoder.fromTsv("output/root_category_indices.tsv")(sc, deploymentMode)

    // Save bus_factor
    Commons.deleteIfExists(sc, busFactorPath)
    Commons.deleteIfExists(sc, busFactorFile)
    result
      .map { case (cat, (busFactor, totalBytes, _)) =>
        val catString = decoder.idxToName(cat)
        s"$catString\t$busFactor\t$totalBytes"
      }
      .coalesce(1)
      .saveAsTextFile(busFactorPath)

    Commons.move(sc, busFactorPath + "/part-00000", busFactorFile)
    Commons.deleteIfExists(sc, busFactorPath)

    // Save top_contributors
    Commons.deleteIfExists(sc, topContributorPath)
    Commons.deleteIfExists(sc, topContribFile)
    result
      .flatMap { case (cat, (_, _, topContributors)) =>
        val catString = decoder.idxToName(cat)
        topContributors.map { case (user, bytes) => s"$catString\t$user\t$bytes" }
      }
      .coalesce(1)
      .saveAsTextFile(topContributorPath)

    Commons.move(sc, topContributorPath + "/part-00000", topContribFile)
    Commons.deleteIfExists(sc, topContributorPath)

    println(s"✓ Saved: $busFactorFile")
    println(s"✓ Saved: $topContribFile")
  }

  case class UserContribution(
    page_id: Int,
    page_title: String,
    event_user_text: String,
    revision_text_bytes_diff: Int
  )
}
