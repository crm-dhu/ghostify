lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.6"
lazy val supportedScalaVersions = List(scala212, scala213)

val sparkVersion = "3.0.0"

val scalaTestArtifact    = "org.scalatest"          %% "scalatest"                % "3.2.+" % Test

val sparkCoreArtifact    = "org.apache.spark"       %% "spark-core"               % sparkVersion % Provided
val sparkSqlArtifact     = "org.apache.spark"       %% "spark-sql"                % sparkVersion % Provided
val sparkMlArtifact      = "org.apache.spark"       %% "spark-mllib"              % sparkVersion % Provided
val scalaNlpArtifact     = "com.johnsnowlabs.nlp"   %% "spark-nlp-silicon"        % "4.3.2"

lazy val commonSettings = Seq(
  scalaVersion := scala212,
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies += scalaTestArtifact,
  organization := "com.vegeta.goku",
  assembly / test := {}  // skip test during assembly
)

assembly / test := {}
assembly / assemblyMergeStrategy := {
  // Work around the duplicated file case, really need to find out and fix the real conflict
  // here, which are most likely brought in by the very lagged behind marketer and event
  // dependencies
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("org.apache.hadoop", _@_*) => MergeStrategy.first
  case PathList("com", "amazonaws", _@_*) => MergeStrategy.last
  case PathList("org", "tensorflow", _@_*) => MergeStrategy.first
  case x if x.startsWith("NativeLibrary") => MergeStrategy.last
  case x if x.startsWith("aws") => MergeStrategy.last
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "ghostify",
    libraryDependencies ++= Seq(
      // Add your dependencies here
    )
  )//.
//  aggregate(djl, spark)

lazy val core = (project in file("ghostify-core")).
  settings(commonSettings: _*).
  settings(
    name := "ghostify-core",
    libraryDependencies ++= Seq(

    )
  )

lazy val spark = (project in file("ghostify-spark")).
  settings(commonSettings: _*).
  settings(
    name := "ghostify-spark",
    libraryDependencies ++= Seq(
      sparkCoreArtifact,
      sparkSqlArtifact,
      sparkMlArtifact,
      scalaNlpArtifact
    )
  )
