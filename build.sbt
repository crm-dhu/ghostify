lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.6"
lazy val supportedScalaVersions = List(scala212, scala213)

val sparkVersion = "3.0.0"
val tensorflowVersion = "0.4.4"
val rocksdbjniVersion = "6.29.5"
val awsSdkVersion = "1.11.828"
val liblevenshteinVersion = "3.0.0"
val greexVersion = "1.0"
val gcpStorageVersion = "2.16.0"
val circeVersion = "0.14.5"

val scalaTestArtifact    = "org.scalatest"                  %% "scalatest"                % "3.2.+" % Test
val sparkCoreArtifact    = "org.apache.spark"               %% "spark-core"               % sparkVersion % Provided
val sparkSqlArtifact     = "org.apache.spark"               %% "spark-sql"                % sparkVersion % Provided
val sparkMlArtifact      = "org.apache.spark"               %% "spark-mllib"              % sparkVersion % Provided

val scalaNlpArtifact     = "com.johnsnowlabs.nlp"           %% "spark-nlp-silicon"        % "4.3.2"

val tensorflow           = "com.johnsnowlabs.nlp"           %% "tensorflow-m1"            % tensorflowVersion
val rocksdbjni           = "org.rocksdb"                    % "rocksdbjni"                % rocksdbjniVersion
val awsSdkS3             = "com.amazonaws"                  % "aws-java-sdk-s3"           % awsSdkVersion
val awsSdkPi             = "com.amazonaws"                  % "aws-java-sdk-pi"           % awsSdkVersion
val liblevenshtein       = "com.github.universal-automata"  % "liblevenshtein"            % liblevenshteinVersion
val greex                = "com.navigamez"                  % "greex"                     % greexVersion

enablePlugins(ClassDiagramPlugin)

val circeDeps = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

lazy val commonSettings = Seq(
  scalaVersion := scala212,
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies += scalaTestArtifact,
  organization := "com.salesforce.mce",
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
      sparkCoreArtifact,
      sparkSqlArtifact,
      sparkMlArtifact,
//      scalaNlpArtifact
       rocksdbjni,
       awsSdkS3,
       awsSdkPi,
       liblevenshtein
         exclude("com.google.guava", "guava")
         exclude("org.apache.commons", "commons-lang3")
         exclude("com.google.code.findbugs", "annotations")
         exclude("org.slf4j", "slf4j-api"),
      greex,
      tensorflow
    ) ++ circeDeps

  )
