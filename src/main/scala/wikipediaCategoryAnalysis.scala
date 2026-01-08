import helper.RootMapping
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Try

//noinspection ZeroIndexToHead
object wikipediaCategoryAnalysis {

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

  private def parseLinktarget(sc: SparkContext, linktarget_path: String): RDD[LinkTarget] =
    // Schema: (lt_id, lt_namespace, lt_title) -> Indices 0, 1, 2

    sc.textFile(linktarget_path)
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
  ): RDD[CategoryLink] =
    // Schema: (cl_from, cl_sortkey, cl_timestamp, prefix, cl_type, collation, cl_target_id)
    // Indices: 0, 1, 2, 3, 4, 5, 6
    // We need: 0 (from), 4 (type), 6 (target_id)

    sc.textFile(categorylinks_path)
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

  private def parsePage(sc: SparkContext, page_path: String): RDD[Page] =
    // Schema: (page_id, page_namespace, page_title, ...) -> Indices 0, 1, 2

    sc.textFile(page_path)
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
      .collect()
      .toMap
  }

  // ---------------------------------------------------------------------------
  // JOB 5: Propagate Labels and Assign to Articles (Optimized with Broadcast)
  // ---------------------------------------------------------------------------
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def buildPageToRootsMap(
    K: Int, // Max number of roots per article/category
    linkTargetRDD: RDD[LinkTarget],
    categoryLinksRDD: RDD[CategoryLink],
    pageRDD: RDD[Page],
    sc: SparkContext
  ): RDD[(Int, Set[String])] = {


    pageRDD.persist(StorageLevel.MEMORY_AND_DISK)
    categoryLinksRDD.persist(StorageLevel.MEMORY_AND_DISK)
    // linkTargetRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    // A. Identify Roots
    val rootMap = identifyRootCategories(linkTargetRDD, categoryLinksRDD, pageRDD)
    println(s"Roots: ${rootMap.mkString(", ")}")
    // linkTargetRDD.unpersist()

    type RootMask = Long
    val rootIdx: Map[Int, Int] = rootMap.keys.zipWithIndex.toMap // rootLtId -> 0..N-1
    sc.parallelize(rootIdx.map { case (ltId, idx) => s"$ltId\t$idx"}.toSeq).saveAsTextFile("output/root_category_indices.tsv")

    @inline def maskOf(idx: Int): RootMask = 1L << idx

    @inline def bitCount(m: RootMask): Int   = java.lang.Long.bitCount(m)
    @inline def isFull(m: RootMask): Boolean = bitCount(m) >= K

    val idxToName: Array[String] = {
      val byIdx = rootIdx.toSeq.sortBy(_._2) // (ltId, idx) sorted by idx
      byIdx.map { case (ltId, _) => rootMap(ltId)._2 }.toArray
    }

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

    def mergeMasks(m1: RootMask, m2: RootMask): RootMask = {
      val combined = m1 | m2
      if (bitCount(combined) <= K) combined
      else m1 // or m2, or some deterministic subset logic
    }


    val pg       = pageRDD.map(pg => (pg.page_id, pg.page_title))
    val lt       = linkTargetRDD.map(lt => (lt.lt_title, lt.lt_id))
    // B. Propagate on Category Skeleton (Iterative)
    val skeleton = categoryLinksRDD
      .filter(_.cl_type == "subcat")
      .map(cl => (cl.cl_from, cl.cl_target_id))
      .join(pg)
      .map { case (id, (targetId, title)) => (title, (id, targetId)) }
      .join(lt)
      .map { case (_, ((page_id, targetId), page_LtId)) => (targetId, (page_id, page_LtId)) }
// Skeleton: (Parent_LtId, (Page_Id, Page_LtId))

    pageRDD.unpersist()

    skeleton.persist(StorageLevel.MEMORY_AND_DISK)
    val skellyCount = skeleton.count()

    printf("Category Skeleton has %d edges.\n", skellyCount)

    var activeFrontier: RDD[(Int, (Int, RootMask))] = sc.parallelize(
      rootMap.toSeq.map { case (ltId, (pageId, _)) => (ltId, (pageId, maskOf(rootIdx(ltId)))) }
    )

    var allAssignments = activeFrontier

    var iteration = 0
    var count     = rootMap.size.toLong
    println("Entering iterative label propagation...")

    // allAssignments: RDD[(ltId, (pageId, mask))]
    // skeleton:       RDD[(ltId, (childPageId, childLtId))]

    while (count > 0 && iteration < 20) {
      iteration += 1
      printf(" Iteration %d: Active Frontier Size = %d\n", iteration, count)

      // 1) Propagate one step
      val propagated = activeFrontier
        .join(skeleton) // (ltId, ((pageId, mask), (childPageId, childLtId)))
        .map { case (_, ((_, parentMask), (childPageId, childLtId))) =>
          (childLtId, (childPageId, parentMask))
        }
        .coalesce(256)

      val updated = propagated
        .leftOuterJoin(allAssignments) // (ltId, ((pageId, newMask), optExisting))
        .flatMap { case (ltId, ((pageId, newMask), optExisting)) =>
          optExisting match {
            // Node already has some roots
            case Some((_, existingMask)) =>
              // If already full, ignore any new roots
              if (isFull(existingMask)) {
                List.empty[(Int, (Int, RootMask))]
              } else {
                val combined = existingMask | newMask
                // If nothing new, or we exceed K, skip
                if (combined == existingMask || bitCount(combined) > K)
                  List.empty[(Int, (Int, RootMask))]
                else
                  List((ltId, (pageId, combined)))
              }
            case None                    => // brand-new node
              List((ltId, (pageId, newMask)))
          }
        }
      activeFrontier.unpersist()
      activeFrontier = updated
      activeFrontier.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

      count = activeFrontier.count()
      if (count > 0) {
        // Merge into allAssignments, 1 row per ltId
        val merged = allAssignments
          .union(activeFrontier)
          .coalesce(256) // Reduce shuffle partitions
          .reduceByKey { case ((pageId1, m1), (_, m2)) =>
            (pageId1, mergeMasks(m1, m2))
          }

        allAssignments = merged

        if (iteration % 3 == 0) {
          printf("  → Checkpointing at iteration %d...\n", iteration)
          val cp = allAssignments
          cp.checkpoint()
          cp.count() // materialize checkpoint

          allAssignments = cp // rebind to short-lineage RDD
          printf("  ✓ Checkpoint complete\n")
        }
      }
    }
    activeFrontier.unpersist()

    printf("Completed a total of %d categories of %d total.\n", allAssignments.count(), skellyCount)

    skeleton.unpersist()
    allAssignments.persist(StorageLevel.MEMORY_AND_DISK)

    // C. Collect Category->Roots Map to Driver (The Optimization)
    // We pre-aggregate here so the map is smaller (CatID -> Set[Roots])
    println("Collecting category labels for Broadcast...")
    val catToRootsLocal = allAssignments
      .map { case (ltId, (_, cat)) => (ltId, cat) }
      .collectAsMap() // Brings ~200-400MB to Driver

    val catToRootsBc = sc.broadcast(catToRootsLocal)

    // D. Map-Side Join with Huge Article Links (No Shuffle)
    println("Broadcasting labels and mapping articles...")
    val finalPageRoots = categoryLinksRDD
      .filter(_.cl_type == "page")
      .mapPartitions { iter =>
        val lookup = catToRootsBc.value // Access Broadcast

        iter.flatMap { cl =>
          // cl.cl_target_id is the Parent Category
          // Check if this parent has any Root Labels
          lookup.get(cl.cl_target_id) match {
            case Some(roots) => List((cl.cl_from, decode(roots))) // (ArticleID, Set[Roots])
            case None        => Nil
          }
        }
      }
      // Final Merge: If Article is in Cat A (Arts) and Cat B (History)
      .reduceByKey(_ ++ _)

    printf(
      "Mapped a total of %d articles out of %d category links.\n",
      finalPageRoots.count(),
      categoryLinksRDD.filter(_.cl_type == "page").count()
    )
    categoryLinksRDD.unpersist()

    // B) Category-side mapping from allAssignments (ltId → pageId, mask)
    val categoryRoots: RDD[(Int, Set[String])] = allAssignments
      .map { case (_, (pageId, mask)) => (pageId, decode(mask)) }
      .reduceByKey(_ ++ _)

    allAssignments.unpersist()

    finalPageRoots.union(categoryRoots)

  }

  // ========== MAIN ==========

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WikipediaCategoryAnalysis")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")
      .set("spark.shuffle.manager", "sort")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")
      .set("spark.io.compression.codec", "lz4")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("checkpoints")

    Logger.getLogger("org.apache.spark.storage.MemoryStore").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val linktarget_path    = "dataset/categories_dumps/enwiki-20251201-linktarget.sql.bz2"
    val categorylinks_path = "dataset/categories_dumps/enwiki-20251201-categorylinks.sql.bz2"
    val page_path          = "dataset/categories_dumps/enwiki-20251201-page.sql.bz2"

    try {
      // 1. Load Data
      val linktarget    = parseLinktarget(sc, linktarget_path)
      val categorylinks = parseCategorylinks(sc, categorylinks_path)
      val page          = parsePage(sc, page_path)


      // 2. Build Graph
      val hierarchy = buildPageToRootsMap(3, linktarget, categorylinks, page, sc)

      //val sample    = hierarchy.take(20)
      //println("\nSample Page to Root Categories Mapping:")
      //sample.foreach { case (pageId, roots) =>
      //  println(s"Page ID: $pageId -> Roots: ${roots.mkString(", ")}")
      //}


      // 3. Save Results

    } finally sc.stop()
  }
}
