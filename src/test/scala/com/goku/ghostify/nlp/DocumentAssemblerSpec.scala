package com.goku.ghostify.nlp

import com.goku.ghostify.PortalPipeline
import com.goku.ghostify.data.{FeatureMap, NamedFeature}
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.AnnotatorType.DOCUMENT
import org.scalatest.wordspec.AnyWordSpec

class DocumentAssemblerSpec extends AnyWordSpec {

  "document assembler" should {

    val documentAssembler =
      DocumentAssembler(NamedFeature[String]("text"), NamedFeature[Array[Annotation]]("document"))

    val pipeline = PortalPipeline(Seq(documentAssembler))
    val text = "This is a test"
    val featureMap = FeatureMap().put(NamedFeature("text"), text)

    val transformed = pipeline.transform(featureMap)
    println(
      transformed.get(NamedFeature[Array[Annotation]]("document")).get === Seq(
        Annotation(DOCUMENT, 0, text.length - 1, text, Map("sentence" -> "0"))
      )
    )
  }
}
