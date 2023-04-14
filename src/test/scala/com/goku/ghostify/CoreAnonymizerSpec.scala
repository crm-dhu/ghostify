package com.goku.ghostify

import org.scalatest.wordspec.AnyWordSpec

class CoreAnonymizerSpec extends AnyWordSpec {

  "Core Anonymizer" should {

    val data = Seq(
      "Google has announced the release of a beta version of the popular TensorFlow machine learning library",
      "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
      "My name is Alex Wang and my email address is alex.wang2009@gmail.com.",
      "Happy birthday, Alex!"
    )

    "correctly anonymize with HF models" in {

      val expected = Seq(
        "[ORG] has announced the release of a beta version of the popular [MISC] machine learning library",
        "The [LOC] metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "My name is [PER] and my email address is [EMAIL].",
        "Happy birthday, [PER]!"
      )

      val results = CoreAnonymizer.predict(data)
      assert(results === expected)

    }
  }
}
