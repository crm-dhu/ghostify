package com.vegeta.goku.ghostify

import org.apache.spark.sql.SparkSession
import org.scalatest.wordspec.AnyWordSpec

class AnonymizerSpec extends AnyWordSpec {

  "Anonymizer" should {

    implicit val sparkSession: SparkSession = TestSparkCluster.session

    val data = sparkSession.sparkContext.parallelize(
      Seq(
        "Google has announced the release of a beta version of the popular TensorFlow machine learning library",
        "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "My name is Alex Wang and my email address is alex.wang2009@gmail.com.",
        "Happy birthday, Alex!"
      )
    )

    "correctly anonymize with default models" in {

      val expected = Seq(
        "[ORG] has announced the release of a beta version of the popular [ORG] machine learning library",
        "The [LOC] metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "My name is [PER] and my email address is [EMAIL].",
        "Happy birthday, [PER]"
      )

      val results = Anonymizer(data, true)
      val processed = results.collect()
      expected.zip(processed).foreach { case (e, p) => assert(e === p) }

    }

    "correctly anonymize with HF models" in {

      val expected = Seq(
        "[ORG] has announced the release of a beta version of the popular [MISC] machine learning library",
        "The [LOC] metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "My name is [PER] and my email address is [EMAIL].",
        "Happy birthday, [PER]!"
      )

      val results = Anonymizer(data, false)
      val processed = results.collect()
      expected.zip(processed).foreach {case (e, p) => assert(e === p)}

    }

  }

}
