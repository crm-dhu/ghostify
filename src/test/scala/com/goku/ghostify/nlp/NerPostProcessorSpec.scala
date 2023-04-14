package com.goku.ghostify.nlp

import com.goku.ghostify.common.{NerMerged, NerTagged}
import org.scalatest.wordspec.AnyWordSpec

class NerPostProcessorSpec extends AnyWordSpec {

  "NER postprocessor" should {

    "merge ner correctly" in {
      val data = Seq(
        NerTagged("year", "year", "O", 0.9998809695243835, true, 10, 13),
        NerTagged(",", ",", "O", 0.9999616146087646, true, 14, 14),
        NerTagged("Alex", "Alex", "B-PER", 0.9996305704116821, true, 16, 19),
        NerTagged("Wang", "Wang", "I-PER", 0.998275637626648, true, 21, 24),
        NerTagged("!", "!", "O", 0.9999642968177795, true, 25, 25)
      )

      val processed = NerPostProcessor.decode(data)
      val expected = Seq(NerMerged("PER",0.998953104019165,16,24))
      assert(processed === expected)

    }
  }

}
