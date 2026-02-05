import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.fs.{ FileSystem, Path }
import utils.Commons

import scala.util.{ Random, Try }

//noinspection ZeroIndexToHead
object NonOptimized_wikipediaCategoryAnalysis {

  // ================= CONSTANTS ==========
  private val bannedCategories: Set[String] = Set(
    "History",
    "Humanities",
    "Time",
    "Life",
    "Behavior",
  )

  // ========== TYPES ==========

  // LinkTarget: (ID, Namespace, Title) - Schema confirmed
  case class LinkTarget(lt_id: Int, lt_namespace: Int, lt_title: String)

  // CategoryLink: (From_Page_ID, Type, Target_Category_ID)
  // CHANGED: cl_target_id is now Int (Foreign Key to LinkTarget)
  case class CategoryLink(cl_from: Int, cl_type: String, cl_target_id: Int)

  // Page: (ID, Title) - Schema confirmed
  case class Page(page_id: Int, page_title: String)

  // HierarchyNode: (ID, Title, Children_IDs)
  case class HierarchyNode(page_id: Int, page_title: String, childs_id: Set[Int])

  // ========== HELPER FUNCTIONS ==========
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def saveSingleTextFile(
    rdd: RDD[String],
    dirPath: String,
    finalFile: String
  )(implicit deploymentMode: String, writeRule: Int, sc: SparkContext): Unit = {
    val finalFilePath = Commons.getDatasetPath(deploymentMode, finalFile)
    val dirPathFull   = Commons.getDatasetPath(deploymentMode, dirPath)

    if (writeRule == 1 && Commons.exists(sc, finalFilePath)) {
      println(s"Output file $finalFile already exists. Skipping save as per write rule.")
      return
    }

    // Delete both temp dir and final file if they exist
    Commons.deleteIfExists(sc, dirPathFull)
    Commons.deleteIfExists(sc, finalFilePath)

    // Save to temp directory
    rdd
      .coalesce(1)
      .saveAsTextFile(dirPathFull)

    // Move the part file to final location using Commons (which handles FS correctly)
    val tempPartPath = dirPathFull + "/part-00000"
    Commons.move(sc, tempPartPath, finalFilePath)

    // Clean up temp directory
    Commons.deleteIfExists(sc, dirPathFull)

    println(s"✓ Saved single file: $finalFilePath")
  }

  // ========== PARSING LOGIC ==========

  /** Robust parser for Splittable LZ4 SQL Dumps. Handles:
    *   1. Header lines (skips them) 2. First data line with "INSERT INTO ... VALUES ..." prefix 3.
    *      Standard data lines "(...)"
    */
  private def extractSqlValues(line: String): Seq[Seq[String]] = {
    var content = line.trim

    // 1. Fast Skip for Metadata/Header lines
    if (
      content.isEmpty ||
      content.startsWith("--") ||
      content.startsWith("/*") ||
      content.startsWith("/*!") ||
      content.startsWith("DROP") ||
      content.startsWith("CREATE") ||
      content.startsWith("LOCK") ||
      content.startsWith("UNLOCK")
    ) {
      return Seq.empty
    }

    // 2. Handle the very first data line which includes "INSERT INTO ... VALUES"
    // Example: INSERT INTO `table` VALUES (1,'A'),(2,'B')
    // Our Python splitter kept this line mostly intact but chopped the VALUES list.
    if (content.startsWith("INSERT INTO")) {
      val valuesIdx = content.indexOf("VALUES")
      if (valuesIdx != -1) {
        // Skip "VALUES" and everything before it
        content = content.substring(valuesIdx + 6).trim
      }
    }

    // 3. Remove trailing semicolon if present (end of file)
    if (content.endsWith(";")) {
      content = content.dropRight(1).trim
    }

    // 4. Basic Validation: Must look like a tuple "(...)"
    if (!content.startsWith("(") || !content.endsWith(")")) {
      return Seq.empty
    }

    // 5. Parse the tuple content
    // Remove outer parens: (1, 'A') -> 1, 'A'
    val tupleBody = content.substring(1, content.length - 1)

    val fields = parseTuple(tupleBody)
    if (fields.nonEmpty)
      Seq(fields)
    else
      Seq.empty
  }

  /** Parses comma-separated values respecting single quotes and escapes. Handles: 'O\'Reilly',
    * 'Text with , inside', etc.
    */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def parseTuple(tuple: String): Seq[String] = {
    import scala.collection.mutable
    val currentVal = new StringBuilder
    val fields     = mutable.ArrayBuffer[String]()
    var inString   = false
    var escapeNext = false

    for (ch <- tuple)
      if (escapeNext) {
        currentVal.append(ch)
        escapeNext = false
      } else if (ch == '\\') {
        escapeNext = true
      } else if (ch == '\'' && !inString) {
        inString = true
      } else if (ch == '\'' && inString) {
        inString = false
      } else if (ch == ',' && !inString) {
        fields += currentVal.result().trim
        currentVal.clear()
      } else {
        currentVal.append(ch)
      }
    // flush last field
    val last = currentVal.result().trim
    if (last.nonEmpty)
      fields += last
    fields.toSeq
  }

  // ========== JOB 1: PARSE LINKTARGET ==========

  private def parseLinktarget(
    sc: SparkContext,
    linktarget_path: String
  )(implicit deploymentMode: String): RDD[LinkTarget] =
    // Schema: (lt_id, lt_namespace, lt_title) -> Indices 0, 1, 2

    sc.textFile(Commons.getDatasetPath(deploymentMode, linktarget_path))
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 3)
          false
        else {
          // Namespace 14 = Category
          row(1) == "14" &&
          Try(row(0).toInt).isSuccess
        }
      }
      .map(row => LinkTarget(row(0).toInt, row(1).toInt, row(2)))

  // ========== JOB 2: PARSE CATEGORYLINKS ==========

  private def parseCategorylinks(
    sc: SparkContext,
    categorylinks_path: String
  )(implicit deploymentMode: String): RDD[CategoryLink] =
    // Schema: (cl_from, cl_sortkey, cl_timestamp, prefix, cl_type, collation, cl_target_id)
    // Indices: 0, 1, 2, 3, 4, 5, 6
    // We need: 0 (from), 4 (type), 6 (target_id)

    sc.textFile(Commons.getDatasetPath(deploymentMode, categorylinks_path))
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 7)
          false
        else {
          val cl_type = row(4)
          // We want 'subcat' (category structure) and 'page' (articles in categories)
          (cl_type == "page" || cl_type == "subcat") &&
          Try(row(0).toInt).isSuccess &&
          Try(row(6).toInt).isSuccess
        }
      }
      .map(row => CategoryLink(row(0).toInt, row(4), row(6).toInt))

  // ========== JOB 3: PARSE PAGE ==========

  private def parsePage(sc: SparkContext, page_path: String)(
    implicit deploymentMode: String
  ): RDD[Page] =
    // Schema: (page_id, page_namespace, page_title, ...) -> Indices 0, 1, 2

    sc.textFile(Commons.getDatasetPath(deploymentMode, page_path))
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 3)
          false
        else {
          // Namespace 0 = Main Article
          row(1) == "0" || row(1) == "14" &&
          Try(row(0).toInt).isSuccess
        }
      }
      .map(row => Page(row(0).toInt, row(2)))

  // ---------------------------------------------------------------------------
  // JOB 4: Identify Root Categories (Children of "Main_topic_classifications")
  // ---------------------------------------------------------------------------
  // Finds all categories directly under "Main_topic_classifications"
  // Returns Map of (LinkTargetID -> (page_id, CategoryName))
  private def identifyRootCategories(
    ltRDD: RDD[LinkTarget],
    clRDD: RDD[CategoryLink],
    pgRDD: RDD[Page]
  ): Map[Int, (Int, String)] = {

    val mainTopicName = pgRDD.filter(_.page_id == 7345184).map(_.page_title).collect().head
    val mainTopicId   = ltRDD.filter(_.lt_title == mainTopicName).map(_.lt_id).collect().head
    println(s"Identified Main Topic Category: $mainTopicName (linktarget ID: $mainTopicId)")

    val rootsId = clRDD
      .filter(cl => cl.cl_target_id == mainTopicId && cl.cl_type == "subcat")
      .map(cl => cl.cl_from)
      .collect()

    val rootsName = pgRDD
      .filter(pg => rootsId.contains(pg.page_id))
      .map(rn => (rn.page_title, rn.page_id))

    ltRDD
      .map(lt => (lt.lt_title, lt.lt_id))
      .join(rootsName)
      .map { case (title, (lt_id, page_id)) => (lt_id, (page_id, title)) }
      .filter { case (_, (_, title)) => !bannedCategories.contains(title) }
      .collect()
      .toMap
  }

  type RootMask = Long

  // ---------------------------------------------------------------------------
  // JOB 5: NON-OPTIMIZED BASELINE VERSION
  // ---------------------------------------------------------------------------
  // This version intentionally uses inefficient patterns for comparison:
  // 1. NO broadcast joins (uses regular joins with shuffles)
  // 2. NO intelligent persistence/unpersistence
  // 3. Uses groupByKey instead of reduceByKey where possible
  // 4. NO coalesce to reduce partitions
  // 5. Keeps checkpointing (necessary for lineage management)
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def buildPageToRootsMap_baseline(
    K: Int, // Max number of roots per article/category
    linkTargetRDD: RDD[LinkTarget],
    categoryLinksRDD: RDD[CategoryLink],
    pageRDD: RDD[Page],
    sc: SparkContext
  )(implicit deploymentMode: String, writeRule: Int): RDD[(Int, RootMask)] = {

    // ❌ NO PERSISTENCE - will recompute multiple times
    
    // A. Identify Roots
    val rootMap = identifyRootCategories(linkTargetRDD, categoryLinksRDD, pageRDD)
    println(s"Roots: ${rootMap.mkString(", ")}")

    val rootIdx: Map[Int, Int] = rootMap.keys.zipWithIndex.toMap // rootLtId -> 0..N-1

    val indices = sc
      .parallelize(
        rootMap
          .keySet
          .intersect(rootIdx.keySet)
          .map(k => k -> (rootMap(k), rootIdx(k)))
          .toSeq
      )
      .map { case (_, ((pageId, title), idx)) => s"$pageId\t$title\t$idx" }

    saveSingleTextFile(
      indices,
      "output_baseline/root_category_indices",
      "output_baseline/root_category_indices.tsv"
    )(deploymentMode, writeRule, sc)

    @inline def maskOf(idx: Int): RootMask = 1L << idx

    @inline def bitCount(m: RootMask): Int   = java.lang.Long.bitCount(m)
    @inline def isFull(m: RootMask): Boolean = bitCount(m) >= K

    def mergeMasks(m1: RootMask, m2: RootMask): RootMask = {
      val combined = m1 | m2
      if (bitCount(combined) <= K)
        combined
      else
        m1
    }

    // ❌ INEFFICIENT: Regular join instead of using already prepared data
    val pg = pageRDD.map(pg => (pg.page_id, pg.page_title))
    val lt = linkTargetRDD.map(lt => (lt.lt_title, lt.lt_id))
    
    // B. Build skeleton with REGULAR JOINS (causes shuffles)
    val skeleton = categoryLinksRDD
      .filter(_.cl_type == "subcat")
      .map(cl => (cl.cl_from, cl.cl_target_id))
      .join(pg) // ❌ SHUFFLE 1: Regular join instead of more efficient approach
      .map { case (id, (targetId, title)) => (title, (id, targetId)) }
      .join(lt) // ❌ SHUFFLE 2: Another regular join
      .map { case (_, ((page_id, targetId), page_LtId)) => (targetId, (page_id, page_LtId)) }

    // ❌ NO PERSISTENCE on skeleton - will be recomputed many times
    val skellyCount = skeleton.count()
    printf("Category Skeleton has %d edges.\n", skellyCount)

    var activeFrontier: RDD[(Int, (Int, RootMask))] = sc.parallelize(
      rootMap.toSeq.map { case (ltId, (pageId, _)) => (ltId, (pageId, maskOf(rootIdx(ltId)))) }
    )

    var allAssignments = activeFrontier

    var iteration = 0
    var count     = rootMap.size.toLong
    println("Entering iterative label propagation (BASELINE - NON-OPTIMIZED)...")

    while (count > 0 && iteration < 20) {
      iteration += 1
      printf(" Iteration %d: Active Frontier Size = %d\n", iteration, count)

      // ❌ INEFFICIENT: Using groupByKey instead of reduceByKey
      val propagated = activeFrontier
        .join(skeleton) // ❌ SHUFFLE: Join without broadcast
        .map { case (_, ((_, parentMask), (childPageId, childLtId))) =>
          (childLtId, (childPageId, parentMask))
        }
        // ❌ NO COALESCE - keeps high partition count even after filtering

      val updated = propagated
        .leftOuterJoin(allAssignments) // ❌ SHUFFLE: Another regular join
        .flatMap { case (ltId, ((pageId, newMask), optExisting)) =>
          optExisting match {
            case Some((_, existingMask)) =>
              if (isFull(existingMask)) {
                List.empty[(Int, (Int, RootMask))]
              } else {
                val combined = existingMask | newMask
                if (combined == existingMask || bitCount(combined) > K)
                  List.empty[(Int, (Int, RootMask))]
                else
                  List((ltId, (pageId, combined)))
              }
            case None                    =>
              List((ltId, (pageId, newMask)))
          }
        }
      
      // ❌ NO UNPERSIST - memory leak over iterations
      activeFrontier = updated
      // ❌ NO PERSISTENCE HERE - will recompute entire lineage

      count = activeFrontier.count()
      
      if (count > 0) {
        // ❌ INEFFICIENT: Using groupByKey instead of reduceByKey
        val merged = allAssignments
          .union(activeFrontier)
          // ❌ NO COALESCE after union
          .groupByKey() // ❌ SHUFFLE: groupByKey instead of reduceByKey (moves all data)
          .mapValues { vals =>
            val (pageId, mask) = vals.head
            val finalMask = vals.map(_._2).reduce(mergeMasks)
            (pageId, finalMask)
          }

        allAssignments = merged

        // ✅ KEEP CHECKPOINTING - necessary for lineage truncation
        if (iteration % 3 == 0) {
          printf("  → Checkpointing at iteration %d...\n", iteration)
          val cp = allAssignments
          cp.checkpoint()
          cp.count() // materialize checkpoint

          allAssignments = cp
          printf("  ✓ Checkpoint complete\n")
        }
      }
    }

    printf("Completed a total of %d categories of %d total.\n", allAssignments.count(), skellyCount)

    // ❌ NO PERSISTENCE on results

    // C. ❌ NO BROADCAST - Regular expensive join
    println("Mapping articles to categories (using regular join - expensive)...")
    
    // Convert allAssignments to (ltId -> mask) for joining
    val categoryMasks = allAssignments
      .map { case (ltId, (_, mask)) => (ltId, mask) }
    
    // ❌ HUGE SHUFFLE: Join millions of article links with category masks
    val finalPageRoots = categoryLinksRDD
      .filter(_.cl_type == "page")
      .map(cl => (cl.cl_target_id, cl.cl_from)) // (categoryLtId, articlePageId)
      .join(categoryMasks) // ❌ MASSIVE SHUFFLE instead of broadcast join
      .map { case (_, (articlePageId, mask)) => (articlePageId, mask) }
      .groupByKey() // ❌ Another groupByKey instead of reduceByKey
      .mapValues(masks => masks.reduce(mergeMasks))
    
    printf(
      "Mapped a total of %d articles out of %d category links.\n",
      finalPageRoots.count(),
      categoryLinksRDD.filter(_.cl_type == "page").count()
    )

    // B) Category-side mapping - also using inefficient pattern
    val categoryRoots: RDD[(Int, RootMask)] = allAssignments
      .map { case (_, (pageId, mask)) => (pageId, mask) }
      .groupByKey() // ❌ groupByKey instead of reduceByKey
      .mapValues(masks => masks.reduce(mergeMasks))

    // Final union and merge
    finalPageRoots.union(categoryRoots)
      .groupByKey() // ❌ Final groupByKey
      .mapValues(masks => masks.reduce(mergeMasks))
  }

  def saveOutputs(
    pageToRootsRDD: RDD[(Int, RootMask)],
    outputPath: String,
  )(implicit sc: SparkContext, deploymentMode: String, writeRule: Int): Unit = {
    val path = Commons.getDatasetPath(deploymentMode, outputPath)
    if (writeRule == 1 && Commons.exists(sc, path)) {
      println(s"Output path $path already exists. Skipping save as per write rule.")
      return
    }
    Commons.deleteIfExists(sc, path)
    pageToRootsRDD
      .map { case (pageId, mask) => s"$pageId\t$mask" }
      .saveAsTextFile(path)
  }

  // ========== MAIN ==========

  def main(args: Array[String]): Unit = {

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
      .setAppName("WikipediaCategoryAnalysis_BASELINE")
      // ❌ NO shuffle optimizations configured
      // The optimized version has these:
      // .set("spark.shuffle.manager", "sort")
      // .set("spark.shuffle.compress", "true")
      // .set("spark.shuffle.spill.compress", "true")
      // .set("spark.io.compression.codec", "lz4")

    implicit val sc: SparkContext = new SparkContext(conf)
    Commons.initializeSparkContext(deploymentMode, sc)
    println("======Starting Wikipedia Category Analysis Job (BASELINE - NON-OPTIMIZED)======")
    sc.setLogLevel("WARN")

    // ✅ KEEP checkpointing - necessary for lineage management
    sc.setCheckpointDir(Commons.getDatasetPath(deploymentMode, "checkpoints_baseline"))

    Logger.getLogger("org.apache.spark.storage.MemoryStore").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val linktarget_path    = "dataset/categories_dump/enwiki-20251201-linktarget.sql.bz2"
    val categorylinks_path = "dataset/categories_dump/enwiki-20251201-categorylinks.sql.bz2"
    val page_path          = "dataset/categories_dump/enwiki-20251201-page.sql.bz2"

    try {

      if (
        writeRule == 1 && Commons.exists(
          sc,
          Commons.getDatasetPath(deploymentMode, "output_baseline/page_to_root_categories/._SUCCESS.crc")
        )
      ) {
        println("Output already exists. Skipping computation as per write rule.")
        return
      }
      
      // 1. Load Data
      val linktarget    = parseLinktarget(sc, linktarget_path)
      val categorylinks = parseCategorylinks(sc, categorylinks_path)
      val page          = parsePage(sc, page_path)

      // 2. Build Graph (BASELINE VERSION)
      val hierarchy = buildPageToRootsMap_baseline(3, linktarget, categorylinks, page, sc)

      // 3. Save Results
      saveOutputs(hierarchy, "output_baseline/page_to_root_categories")

      val sample = hierarchy.takeSample(withReplacement = false, 20, Random.nextLong())

      println("\nSample Page to Root Categories Mapping:")

      val decoder = RootDecoder.fromTsv("output_baseline/root_category_indices.tsv")(sc, deploymentMode)
      sample.foreach { case (pageId, roots) =>
        println(s"Page ID: $pageId -> Roots: ${decoder.decode(roots).mkString(", ")}")
      }

    } finally sc.stop()
  }
}

// Decoder is the same - reuse from optimized version
@SuppressWarnings(Array("org.wartremover.warts.While"))
final case class RootDecoder(idxToName: Vector[String]) {
  private type RootMask = Long

  def decode(mask: RootMask): Set[String] = {
    var i   = 0
    var acc = Set.empty[String]
    val max = idxToName.length
    while (i < max) {
      if ((mask & (1L << i)) != 0L)
        acc += idxToName(i)
      i += 1
    }
    acc
  }

  def categoriesFromMask(mask: Long): Set[Int] = {
    var m   = mask
    var bit = 0
    var res = Set.empty[Int]

    while (m != 0L && bit < 64) {
      if ((m & 1L) != 0L)
        res += bit
      m = m >>> 1
      bit += 1
    }
    res
  }

  def idToString(id: Int): String = idxToName(id)
}

object RootDecoder {
  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  def fromTsv(path: String)(sc: SparkContext, deploymentMode: String): RootDecoder = {
    val lines = sc.textFile(Commons.getDatasetPath(deploymentMode, path))
    val pairs =
      lines
        .map { line =>
          val Array(_, title, idxStr) = line.split("\t", 3)
          (idxStr.toInt, title)
        }
        .collect()
        .toSeq

    val maxIdx    = pairs.map(_._1).max
    val idxToName = Array.fill[String](maxIdx + 1)("")

    pairs.foreach { case (i, name) => idxToName(i) = name }

    RootDecoder(idxToName.toVector)
  }
}
