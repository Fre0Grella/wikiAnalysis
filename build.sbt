name := "WikipediaAnalytics"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",

  "org.slf4j" % "slf4j-api" % "2.0.9"
)

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}

assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { f =>
    f.data.getName.contains("spark-") ||
      f.data.getName.contains("hadoop-") ||
      f.data.getName.contains("scala-library")
  }
}

wartremoverErrors ++= Warts.allBut(
  Wart.Var,                 // Necessary for mutable state in parsers
  Wart.Return,              // Useful for early exit in parsers
  Wart.Throw,               // Necessary for error handling
  Wart.NonUnitStatements,   // Spark's .cache(), .persist() trigger this
  Wart.DefaultArguments,    // Allow default args in functions
  Wart.Any,                 // Spark inference often hits 'Any'
  Wart.Nothing,             // Spark inference often hits 'Nothing'
  Wart.Serializable,        // Spark closures are serializable
  Wart.JavaSerializable,    // Spark closures are serializable
  Wart.Product,             // Case classes inherit Product
  Wart.Equals,
  Wart.SizeIs,
  Wart.SeqApply,
  Wart.FinalCaseClass
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "UTF-8"
)

