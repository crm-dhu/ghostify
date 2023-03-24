package com.goku.ghostify

import org.apache.spark.sql.SparkSession

object TestSparkCluster {

  lazy val session: SparkSession = {
    val session =
      SparkSession
        .builder()
        .config("spark.jsl.settings.aws.region", "us-west-2")
        .master("local[2]")
        .appName(getClass.getSimpleName)
        .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    session
  }
}
