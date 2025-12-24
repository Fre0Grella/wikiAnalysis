import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Try

//noinspection ZeroIndexToHead
object wikipediaCategoryAnalysis {

  // ========== TYPES ==========

  // noinspection ScalaWeakerAccess
  case class LinkTarget(lt_id: Int, lt_namespace: Int, lt_title: String)
  // noinspection ScalaWeakerAccess
  case class CategoryLink(cl_from: Int, cl_type: String, cl_target_id: Int)
  // noinspection ScalaWeakerAccess
  case class Page(page_id: Int, page_title: String)
  // noinspection ScalaWeakerAccess
  case class HierarchyNode(page_id: Int, page_title: String, childs_id: Set[Int])

  // ========== SQL PARSING ==========

  /** Extract VALUES from SQL INSERT statement Example: INSERT INTO linktarget VALUES
    * (1,14,'Biology'),(2,14,'Chemistry'); Returns: Seq(Seq("1","14","Biology"),
    * Seq("2","14","Chemistry"))
    */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  def extractSqlValues(line: String): Seq[Seq[String]] = {
    // Only process INSERTs for this table
    if (!line.startsWith("INSERT INTO `linktarget`") || !line.contains("VALUES")) {
      return Seq.empty
    }

    // Keep only the part after "VALUES"
    val valuesIdx = line.indexOf("VALUES")
    if (valuesIdx == -1) return Seq.empty

    val afterValues = line.substring(valuesIdx + "VALUES".length).trim
    // Drop trailing ';'
    val noSemicolon =
      if (afterValues.endsWith(";")) afterValues.dropRight(1).trim else afterValues

    // Find first '(' and last ')'
    val firstParen = noSemicolon.indexOf('(')
    val lastParen  = noSemicolon.lastIndexOf(')')
    if (firstParen == -1 || lastParen <= firstParen) return Seq.empty

    // Now we have: "(a,b,'c'),(d,e,'f'),..."
    val inside = noSemicolon.substring(firstParen + 1, lastParen) // drop outer parens

    // Split tuples on "),(" safely
    val tupleStrings = inside.split("\\),\\(")

    tupleStrings.toSeq.map(parseTuple)
  }
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def parseTuple(tuple: String): Seq[String] = {
    import scala.collection.mutable
    val currentVal = new StringBuilder
    val fields     = mutable.ArrayBuffer[String]()

    var inString   = false
    var escapeNext = false

    for (ch <- tuple) {
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
    }

    // flush last field
    val last = currentVal.result().trim
    if (last.nonEmpty) fields += last

    fields.toSeq
  }



  // ========== JOB 1: PARSE LINKTARGET ==========

  private def parseLinktarget(sc: SparkContext, linktarget_path: String): RDD[LinkTarget] = {
    println("")
    println("[JOB 1] Parsing linktarget table (categories)")
    println("")


    val raw_rdd = sc
      .textFile(linktarget_path)
      //.filter(line => line.contains("INSERT INTO"))

    val linktarget_rdd = raw_rdd
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 3)
          false
        else {
          row.lift(1).map(_.toInt).getOrElse(-1) == 14 &&
          row.lift(0).map(_.toInt).getOrElse(-1) > 0
        }
      }
      .map(row => LinkTarget(row(0).toInt, row(1).toInt, row(2)))

    linktarget_rdd.cache()


    val count = linktarget_rdd.count()
    println(s"✓ Loaded ${count} categories in ")

    linktarget_rdd
  }

  // ========== JOB 2: PARSE CATEGORYLINKS ==========

  private def parseCategorylinks(
    sc: SparkContext,
    categorylinks_path: String
  ): RDD[CategoryLink] = {
    println("\n")
    println("[JOB 2] Parsing categorylinks table (article↔category links)")
    println("")


    val raw_rdd = sc
      .textFile(categorylinks_path)
      .filter(line => line.contains("INSERT INTO"))

    val categorylinks_rdd = raw_rdd
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 7)
          false
        else {
          val cl_type = row.lift(4).getOrElse("")
          cl_type == "page" || cl_type == "subcat" &&
          row.lift(0).map(_.toInt).getOrElse(-1) > 0 &&
          row.lift(6).map(_.toInt).getOrElse(-1) > 0
        }
      }
      .map(row => CategoryLink(row(0).toInt, row(4), row(6).toInt))
      .filter(t => t.cl_type == "subcat")
      .distinct()

    categorylinks_rdd.cache()


    //val count = categorylinks_rdd.count()
    println(s"✓ Loaded category links in ")

    categorylinks_rdd
  }

  // ========== JOB 3: PARSE PAGE ==========

  private def parsePage(sc: SparkContext, page_path: String): RDD[Page] = {
    println("\n")
    println("[JOB 3] Parsing page table (article metadata)")
    println("")


    val raw_rdd = sc
      .textFile(page_path)
      .filter(line => line.contains("INSERT INTO"))

    val page_rdd = raw_rdd
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 3)
          false
        else {
          row.lift(1).map(_.toInt).getOrElse(-1) == 0 &&
          row.lift(0).map(_.toInt).getOrElse(-1) > 0
        }
      }
      .map(row => Page(row(0).toInt, row(2)))

    page_rdd.cache()


    //val count = page_rdd.count()
    println(s"✓ Loaded articles in ")

    page_rdd
  }

  // ========== JOB 4: BUILD PROPER GRAPH LINK ==========

  private def buildHierarchy(
    linktarget_rdd: RDD[LinkTarget],
    categorylinks_rdd: RDD[CategoryLink],
    page_rdd: RDD[Page]
  ): RDD[HierarchyNode] = {
    println("\n")
    println("[JOB 4] Building category hierarchy graph")
    println("")

    val pageIdLookup = categorylinks_rdd
      .keyBy(_.cl_from)
      .join(page_rdd.keyBy(_.page_id))
      .map(data => (data._2._2.page_title, data._1))
      .join(linktarget_rdd.keyBy(_.lt_title))
      .map(data => (data._2._2.lt_id, data._2._1, data._1))

    val hierarchyNode = categorylinks_rdd
      .keyBy(_.cl_target_id)
      .join(pageIdLookup.keyBy(_._1))
      .map(data => (data._2._1.cl_from, (data._2._2._1, data._2._2._3)))
      .groupBy(_._2)
      .map(data => HierarchyNode(data._1._1, data._1._2, data._2.map(_._1).toSet))
      .cache()

    page_rdd.unpersist()
    categorylinks_rdd.unpersist()
    linktarget_rdd.unpersist()
    //val count   = hierarchyNode.count()
    println(s"✓ Built hierarchy nodes in ")

    hierarchyNode
  }

  // ========== JOB 5: FIND MAIN TOPICS ==========

  private def findMainTopics(
    sc: SparkContext,
    hierarchyNode: RDD[HierarchyNode],
    main_topic_name: String = "Main_topic_classifications"
  ): Set[Page] = {
    println("\n" + "=" * 70)
    println("[JOB 5] Finding main topics")
    println("=" * 70)


    val main_topic_result = hierarchyNode
      .filter(t => t.page_title == main_topic_name)
      .collect()

    if (main_topic_result.isEmpty) {
      throw new Exception(s"'$main_topic_name' not found in categories!")
    }

    println(s"✓ Found Main_topic_classifications")

    val main_topics =
      hierarchyNode
        .filter(n => main_topic_result(0).childs_id.contains(n.page_id))
        .map(n => Page(n.page_id, n.page_title))
        .collect()
        .toSet
    println(s"✓ Found ${main_topics.size} main topics")

    if (main_topics.nonEmpty) {
      println(s"  Main topics:")
      main_topics.foreach(mt => println(s"      - ${mt.page_title} (page_id=${mt.page_id})"))
    }

    println(s"✓ Completed in ")

    sc.broadcast(main_topics)
    main_topics
  }
  /*
  // ========== NEW JOB 5A: PRECOMPUTE CATEGORY MAIN TOPICS (OPTIMIZED) ==========

  /** Precompute main topic for all categories using iterative label propagation.
   *
   * Iterative Label Propagation:
   *   1. Mark direct main topics: (mainTopic_id → mainTopic_id) 2. For K iterations:
   *      - Propagate labels upward through hierarchy
   *      - Propagate labels downward (handles backward cycle edges)
   *      - Union + ReduceByKey ensures convergence
   *      3. Result: (category_id → main_topic_id) for all reachable categories
   *
   * Cycle handling: Built-in via union + reduceByKey (idempotent) Time: O(K × #edges) where K ≈ 10
   * (typical hierarchy depth) Space: O(#categories) distributed across nodes
   */
  def precomputeCategoryMainTopics(
    sc: SparkContext,
    hierarchy_rdd: RDD[(Int, Int)],
    main_topics_set: Set[Int]
  ): RDD[(Int, Int)] = {
    println("\n" + "=" * 70)
    println("[JOB 6A] Precomputing category → main topic (OPTIMIZED)")
    println("=" * 70)
    println("         Using iterative label propagation (Option 1)")


    // Step 1: Mark direct main topics
    val directMainTopics: RDD[(Int, Int)] = sc.parallelize(
      main_topics_set.map(lt_id => (lt_id, lt_id)).toSeq
    )

    // Step 2: Iteratively propagate labels
    var labels: RDD[(Int, Int)] = directMainTopics
    labels.persist()

    val maxIters  = 15 // Tune based on expected hierarchy depth
    var converged = false

    for (iter <- 1 to maxIters if !converged) {
      val beforeCount = labels.count()

      // Propagate upward through parent edges (child → parent direction)
      val parentLabeled: RDD[(Int, Int)] = hierarchy_rdd
        .map { case (child, parent) => (parent, child) } // key by parent
        .join(labels) // (parent, (child, mainTopic))
        .map { case (_, (child, mainTopicId)) => (child, mainTopicId) }

      // Propagate downward through child edges (handles backward cycle edges)
      val childLabeled: RDD[(Int, Int)] = hierarchy_rdd
        .map { case (child, parent) => (child, parent) } // key by child
        .join(labels) // (child, (parent, mainTopic))
        .map { case (_, (parent, mainTopicId)) => (parent, mainTopicId) }

      // Union both directions + keep first label (idempotent, cycle-safe)
      val newLabels = labels
        .union(parentLabeled)
        .union(childLabeled)
        .reduceByKey((a, _) => a) // First label wins, never changes
        .persist()

      val afterCount = newLabels.count()

      labels.unpersist()
      labels = newLabels

        println(
        s"  Iteration $iter: $afterCount categories labeled (new: ${afterCount - beforeCount}) []"
      )

      // Check convergence: no new labels
      if (afterCount == beforeCount) {
        println(s"  ✓ Convergence at iteration $iter")
        converged = true
      }
    }

    val count   = labels.count()
    println(s"✓ Precomputed $count category → main topic mappings in ")

    labels
  }

  // ========== NEW JOB 6B: FILTER HIDDEN CATEGORIES ==========

  /** Remove hidden categories (Stub articles, Maintenance, etc.) using depth-based filtering.
   *
   * Key insight: Hidden categories are typically at shallow depth from main topics. Depth ≥ 2
   * removes most hidden categories.
   *
   * Removes ~25% of categories, cleaning up results significantly.
   */
  def filterHiddenCategories(
    sc: SparkContext,
    catMainTopic: RDD[(Int, Int)],
    hierarchy_rdd: RDD[(Int, Int)],
    linktarget_rdd: RDD[(Int, String)],
    minDepth: Int = 2
  ): RDD[(Int, Int)] = {
    println("\n" + "=" * 70)
    println(s"[JOB 6B] Filtering hidden categories (minDepth=$minDepth)")
    println("=" * 70)


    // Step 1: Compute depth of each category from its main topic
    // Initialize: main topics have depth 0, others have depth 1
    var depths: RDD[(Int, Int)] = catMainTopic
      .map { case (cat, mainTopic) =>
        if (cat == mainTopic) {
          (cat, 0) // Main topic itself is depth 0
        } else {
          (cat, 1) // Others start at depth 1 (will be refined)
        }
      }

    depths.persist()

    // Step 2: Iteratively increase depth by traversing hierarchy
    // Each hop up increases depth
    val maxDepthIters  = 10
    var depthConverged = false

    for (iter <- 1 to maxDepthIters if !depthConverged) {
      val beforeCount = depths.count()

      // For each edge (child, parent): if parent has depth D,
      // child can have depth at most D+1
      val depthIncreased: RDD[(Int, Int)] = hierarchy_rdd
        .map { case (child, parent) => (parent, child) } // key by parent
        .join(depths) // (parent, (child, depth[parent]))
        .map { case (_, (child, parentDepth)) => (child, parentDepth + 1) }

      // Keep maximum depth seen (some categories have multiple parents)
      val newDepths = depths
        .union(depthIncreased)
        .reduceByKey((d1, d2) => math.max(d1, d2))
        .persist()

      val afterCount = newDepths.count()

      depths.unpersist()
      depths = newDepths

      println(s"  Depth iter $iter: updated (convergence check...)")

      if (afterCount == beforeCount) {
        println(s"  ✓ Depth computation converged at iteration $iter")
        depthConverged = true
      }
    }

    // Step 3: Filter by minimum depth
    val filtered = depths
      .filter(_._2 >= minDepth)
      .map(_._1)                // Return just category IDs
      .map(catId => (catId, 1)) // Dummy value for join compatibility

    val beforeCount    = catMainTopic.count()
    val afterCount     = filtered.count()
    val removed        = beforeCount - afterCount
    val removedPercent =
      if (beforeCount > 0)
        (100.0 * removed / beforeCount).toInt
      else
        0

    println(s"✓ Removed $removed hidden categories ($removedPercent%)")
    println(s"  Remaining: $afterCount categories for article mapping")
    println(s"  Completed in ")

    // Return filtered categories (rejoin with catMainTopic to get proper format)
    catMainTopic
      .join(filtered)
      .map { case (cat, (mainTopic, _)) => (cat, mainTopic) }
  }

  // ========== JOB 6C: MAP ARTICLES TO MAIN TOPICS (SIMPLIFIED) ==========

  /** Map articles to main topics using precomputed category→mainTopic mapping. This is now a simple
   * lookup join (no DFS per article).
   *
   * Time: O(#articles) instead of O(#articles × depth) ~7-10x faster than naive approach
   */
  def mapArticlesToMainTopics(
    sc: SparkContext,
    categorylinks_rdd: RDD[(Int, String, Int)],
    page_rdd: RDD[(Int, String)],
    linktarget_rdd: RDD[(Int, String)],
    catMainTopic: RDD[(Int, Int)]
  ): RDD[(Int, String, String, Int)] = {
    println("\n" + "=" * 70)
    println("[JOB 6C] Mapping articles to main topics (simplified join)")
    println("=" * 70)


    // Extract article→category links
    val articleCategories: RDD[(Int, Int)] = categorylinks_rdd
      .filter(_._2 == "page")
      .map { case (pageId, _, catId) => (catId, pageId) }

    // Simple join: (catId, pageId) JOIN (catId, mainTopicId) = (catId, (pageId, mainTopicId))
    val articleMainTopics: RDD[(Int, Int)] = articleCategories
      .join(catMainTopic)
      .map { case (_, (pageId, mainTopicId)) => (pageId, mainTopicId) }

    // Join with page titles
    val withPageTitle = articleMainTopics
      .join(page_rdd)
      .map { case (pageId, (mainTopicId, pageTitle)) => (mainTopicId, (pageId, pageTitle)) }

    // Join with main topic titles
    val result = withPageTitle
      .join(linktarget_rdd)
      .map { case (mainTopicId, ((pageId, pageTitle), mainTopicTitle)) =>
        (pageId, pageTitle, mainTopicTitle, mainTopicId)
      }

    result.persist()
    val count = result.count()

    println(s"✓ Mapped $count articles to main topics in ")

    result
  }

  // ========== OUTPUT ==========

  def writeOutput(result_rdd: RDD[(Int, String, String, Int)], output_path: String): Unit = {
    println(s"\n[OUTPUT] Writing to $output_path...")

    val csv_rdd = result_rdd.map { t =>
      val page_id          = t._1
      val article_title    = t._2.replace(",", "\\,")
      val main_topic       = t._3.replace(",", "\\,")
      val main_topic_lt_id = t._4
      s"$page_id,$article_title,$main_topic,$main_topic_lt_id"
    }

    val output_file = new File(output_path)
    output_file.mkdirs()

    val csv_path = s"$output_path/articles_main_topics.csv"
    csv_rdd.coalesce(1).saveAsTextFile(csv_path)
    println(s"✓ CSV: $csv_path")

    val tsv_rdd = result_rdd.map { t =>
      val page_id          = t._1
      val article_title    = t._2.replace("\t", " ")
      val main_topic       = t._3.replace("\t", " ")
      val main_topic_lt_id = t._4
      s"$page_id\t$article_title\t$main_topic\t$main_topic_lt_id"
    }

    val tsv_path = s"$output_path/articles_main_topics.tsv"
    tsv_rdd.coalesce(1).saveAsTextFile(tsv_path)
    println(s"✓ TSV: $tsv_path")
  }
   */
  // ========== MAIN ==========

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WikipediaCategoryAnalysis")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.driver.maxResultSize", "2g")

    val sc = new SparkContext(conf)

    val linktarget_path    = "dataset/sql_dumps/*-linktarget.sql.gz"
    //val linktarget_path    = "dataset/sql_dumps/test.txt"
    val categorylinks_path = "dataset/sql_dumps/*-categorylinks.sql.gz"
    val page_path          = "dataset/sql_dumps/*-page.sql.gz"
    val output_path        = "dataset/categories/articles_main_topics_tuple.tsv"

    try {

      val pipeline_start = System.currentTimeMillis()

      // Parse input files
      val linktarget_rdd    = parseLinktarget(sc, linktarget_path)
      linktarget_rdd.take(10).foreach(data => println(s"  Sample LinkTarget: $data"))
      val categorylinks_rdd = parseCategorylinks(sc, categorylinks_path)
      categorylinks_rdd.take(10).foreach(data => println(s"  Sample CategoryLink: $data"))
      val page_rdd          = parsePage(sc, page_path)
      page_rdd.take(10).foreach(data => println(s"  Sample Page: $data"))

      // Build hierarchy
      val hierarchy_rdd = buildHierarchy(linktarget_rdd, categorylinks_rdd, page_rdd)
      hierarchy_rdd.take(10).foreach(data => println(s"  Sample HierarchyNode: $data"))

      // Find main topics
      val main_topics_set = findMainTopics(sc,hierarchy_rdd)
      main_topics_set.take(10).foreach(data => println(s"  Sample Main Topic: $data"))

      /*// NEW JOB 6A: Precompute category → main topic (cycle-safe, handles cycles automatically)
      val catMainTopic = precomputeCategoryMainTopics(
        sc, hierarchy_rdd, main_topics_set
      )

      // NEW JOB 6B: Filter hidden categories (depth-based filtering)
      val catMainTopicFiltered = filterHiddenCategories(
        sc, catMainTopic, hierarchy_rdd, linktarget_rdd,
        minDepth = 2  // Tune this (0=baseline, 1=aggressive, 2=recommended, 3=very aggressive)
      )

      // JOB 6C: Map articles (now trivial - just joins)
      val result_rdd = mapArticlesToMainTopics(
        sc, categorylinks_rdd, page_rdd, linktarget_rdd, catMainTopicFiltered
      )

      // Write output
      writeOutput(result_rdd, output_path)
       */
      val elapsed = (System.currentTimeMillis() - pipeline_start) / 1000.0
      println("\n" + "=" * 70)
      println("✓ ALL JOBS COMPLETED SUCCESSFULLY")
      println(f"  Total time: $elapsed%.1fs (${elapsed / 60}%.1fm)")
      println("  Optimization: Option 1 (Iterative Label Propagation)")
      println("  - Cycle handling: Built-in via union+reduceByKey")
      println("  - Hidden categories: Removed via depth-based filtering")
      println("=" * 70 + "\n")

    } catch {
      case e: Exception =>
        println(s"\n❌ FATAL ERROR: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally sc.stop()
  }
}
