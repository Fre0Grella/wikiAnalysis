import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

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
  def extractSqlValues(line: String): Seq[Seq[String]] = {
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

  private def parseLinktarget(sc: SparkContext, linktarget_path: String): RDD[LinkTarget] = {
    println("\n[JOB 1] Parsing linktarget table (categories)")

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
  }

  // ========== JOB 2: PARSE CATEGORYLINKS ==========

  private def parseCategorylinks(
    sc: SparkContext,
    categorylinks_path: String
  ): RDD[CategoryLink] = {
    println("\n[JOB 2] Parsing categorylinks table")

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
  }

  // ========== JOB 3: PARSE PAGE ==========

  private def parsePage(sc: SparkContext, page_path: String): RDD[Page] = {
    println("\n[JOB 3] Parsing page table")

    // Schema: (page_id, page_namespace, page_title, ...) -> Indices 0, 1, 2

    sc.textFile(page_path)
      .flatMap(line => extractSqlValues(line))
      .filter { row =>
        if (row.length < 3)
          false
        else {
          // Namespace 0 = Main Article
          row(1) == "0" &&
          Try(row(0).toInt).isSuccess
        }
      }
      .map(row => Page(row(0).toInt, row(2)))
  }

  // ========== JOB 4: BUILD HIERARCHY ==========

  private def buildHierarchy(
    linktarget_rdd: RDD[LinkTarget],
    categorylinks_rdd: RDD[CategoryLink],
    page_rdd: RDD[Page]
  ): RDD[HierarchyNode] = {

    println("\n[JOB 4] Building hierarchy graph (Corrected & Optimized)")

    // ---------------------------------------------------------
    // STEP 1: Aggregate Children
    // ---------------------------------------------------------
    // We first shrink the 100M+ rows of links into ~2M rows of (ParentRef, Set[Children])
    // We use the ID available in CategoryLinks (lt_id) to do this heavy lifting.
    val rawLinks = categorylinks_rdd.map(cl => (cl.cl_target_id, cl.cl_from))

    @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
    val groupedById = rawLinks
      .aggregateByKey(mutable.HashSet.empty[Int])(
        (set, childId) => set += childId,
        (set1, set2) => set1 ++= set2
      )
      .mapValues(_.toSet)
    // Result: RDD[(lt_id, Set[Child_Page_IDs])]
    // We have reduced the data massively here. No OOM.

    // ---------------------------------------------------------
    // STEP 2: Resolve Parent Name (LinkTarget Join)
    // ---------------------------------------------------------
    // Now we translate the internal 'lt_id' to the string 'Title'
    val ltMap = linktarget_rdd.map(lt => (lt.lt_id, lt.lt_title))

    val groupedByTitle = groupedById
      .join(ltMap)
      .map { case (lt_id, (children, title)) => (title, children) }
    // Result: RDD[(String_Title, Set[Child_Page_IDs])]

    // ---------------------------------------------------------
    // STEP 3: Resolve Parent Page ID (Page Join)
    // ---------------------------------------------------------
    // Finally, we use the Title to find the real Page ID.
    val categoryPages = page_rdd
      .map(p => (p.page_title, p.page_id))

    val finalHierarchy = groupedByTitle
      .join(categoryPages)
      .map { case (title, (children, realPageId)) =>
        // Now we have everything: The real ID, the Title, and the Set of children
        HierarchyNode(realPageId, title, children)
      }

    finalHierarchy
  }

  // ========== JOB 5: FIND MAIN TOPICS ==========

  private def findMainTopics(
    sc: SparkContext,
    hierarchyNode: RDD[HierarchyNode],
    main_topic_name: String = "Main_topic_classifications"
  ): Set[Page] = {
    println("\n" + "=" * 70)
    println(s"[JOB 5] Finding main topics under '$main_topic_name'")
    println("=" * 70)

    // Find the node for "Main_topic_classifications"
    val rootNode = hierarchyNode
      .filter(t => t.page_title == main_topic_name)
      .collect()

    if (rootNode.isEmpty) {
      println(s"⚠️ Warning: '$main_topic_name' not found. Check casing or dataset.")
      return Set.empty
    }

    val root = rootNode(0)
    println(
      s"✓ Found Root: ${root.page_title} (ID=${root.page_id}) with ${root.childs_id.size} children"
    )

    // Get the actual Page objects for these children
    // We can filter the hierarchy again to find nodes that ARE these children
    // (Assuming main topics are also Categories themselves)

    val mainTopics =
      hierarchyNode
        .filter(n => root.childs_id.contains(n.page_id))
        .map(n => Page(n.page_id, n.page_title))
        .collect()
        .toSet

    println(s"✓ Found ${mainTopics.size} main topics:")
    mainTopics.foreach(mt => println(s" - ${mt.page_title}"))

    mainTopics
  }

  // ========== MAIN ==========

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WikipediaCategoryAnalysis")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // PATHS to your new LZ4 files
    val linktarget_path    = "dataset/categories_dumps/enwiki-20251201-linktarget.sql.bz2"
    val categorylinks_path = "dataset/categories_dumps/enwiki-20251201-categorylinks.sql.bz2"
    val page_path          = "dataset/categories_dumps/enwiki-20251201-page.sql.bz2"

    try {
      // 1. Load Data
      val linktarget    = parseLinktarget(sc, linktarget_path)
      // val checklt       = linktarget.count()
      // println(s"✓ LinkTarget parsed with $checklt entries.")
      val categorylinks = parseCategorylinks(sc, categorylinks_path)
      // val checkcl       = categorylinks.count()
      // println(s"✓ CategoryLinks parsed with $checkcl entries.")
      val page          = parsePage(sc, page_path)
      // val checkp        = page.count()
      // println(s"✓ Page parsed with $checkp entries.")

      // 2. Build Graph
      val hierarchy = buildHierarchy(linktarget, categorylinks, page)
      hierarchy.cache()
      println(s"✓ Hierarchy graph built with ${hierarchy.count()} nodes.")

      // 3. Find Main Topics
      val maintopic = findMainTopics(sc, hierarchy)
      val checkmt   = maintopic.size
      println(s"✓ Found $checkmt main topics.")
      maintopic.foreach(mt => println(s" - ${mt.page_title}"))

      // 4. (Optional) Run PageRank or other analysis here...

    } finally sc.stop()
  }
}
