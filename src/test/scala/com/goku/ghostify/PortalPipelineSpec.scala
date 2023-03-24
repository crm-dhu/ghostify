package com.goku.ghostify

import com.goku.ghostify.data.{FeatureMap, NamedFeature}
import com.goku.ghostify.nlp.{DocumentAssembler, SentenceDetector}
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.AnnotatorType.DOCUMENT
import org.scalatest.wordspec.AnyWordSpec

class PortalPipelineSpec extends AnyWordSpec {

  val text = "My name is Alex Wang. My email address is alex.wang@gmail.com."
  val featureMap = FeatureMap().put(NamedFeature("text"), text)
  val documentAssembler =
    DocumentAssembler(NamedFeature[String]("text"), NamedFeature[Array[Annotation]]("document"))
  val sentenceDetector = SentenceDetector(
    NamedFeature[Array[Annotation]]("document"),
    NamedFeature[Array[Annotation]]("sentence")
  )
  val pipeline = PortalPipeline(Seq(documentAssembler, sentenceDetector))
  val transformed = pipeline.transform(featureMap)

  "document assembler" should {

    "return correct answer" in {

      assert(
        transformed.get(NamedFeature[Array[Annotation]]("document")).get === Seq(
          Annotation(DOCUMENT, 0, text.length - 1, text, Map("sentence" -> "0"))
        )
      )
    }
  }

  "sentence detector" should {

    "return correct answer" in {

      val transformed = pipeline.transform(featureMap)
      assert(
        transformed.get(NamedFeature[Array[Annotation]]("sentence")).get === Array(
          Annotation(DOCUMENT, 0, 20, "My name is Alex Wang.", Map("sentence" -> "0")),
          Annotation(DOCUMENT, 22, 61, "My email address is alex.wang@gmail.com.", Map("sentence" -> "1"))
        )
      )
    }
  }

}
