package com.goku.ghostify

import com.goku.ghostify.data.{FeatureMap, NamedFeature}
import com.goku.ghostify.nlp.{BertForTokenClassification, DocumentAssembler, NerConverter, SentenceDetector, TokenizerModel}
import com.goku.ghostify.util.Params
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.AnnotatorType.{CHUNK, DOCUMENT, NAMED_ENTITY, TOKEN}
import org.scalatest.wordspec.AnyWordSpec

class PortalPipelineSpec extends AnyWordSpec {

  val text = "My name is Alex Wang. Here is my email: alex.wang@gmail.com."
  val featureMap = FeatureMap().put(NamedFeature("text"), text)
  val documentAssembler =
    DocumentAssembler(
      NamedFeature[String]("text"),
      NamedFeature[Array[Annotation]]("document")
    )
  val sentenceDetector = SentenceDetector(
    NamedFeature[Array[Annotation]]("document"),
    NamedFeature[Array[Annotation]]("sentence")
  )
  val tokenizer =
    TokenizerModel(
      NamedFeature[Array[Annotation]]("sentence"),
      NamedFeature[Array[Annotation]]("token")
    )

  val ner = BertForTokenClassification(
    (NamedFeature[Array[Annotation]]("document"), NamedFeature[Array[Annotation]]("token")),
    NamedFeature[Array[Annotation]]("ner"),
    Params.ModelPath
  )

  val nerConverter = NerConverter(
    (NamedFeature[Array[Annotation]]("document"), NamedFeature[Array[Annotation]]("token"), NamedFeature[Array[Annotation]]("ner")),
    NamedFeature[Array[Annotation]]("predictions")
  )

  val pipeline = PortalPipeline(Seq(documentAssembler, sentenceDetector, tokenizer, ner, nerConverter))
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

      assert(
        transformed.get(NamedFeature[Array[Annotation]]("sentence")).get === Array(
          Annotation(DOCUMENT, 0, 20, "My name is Alex Wang.", Map("sentence" -> "0")),
          Annotation(
            DOCUMENT,
            22,
            59,
            "Here is my email: alex.wang@gmail.com.",
            Map("sentence" -> "1")
          )
        )
      )
    }
  }

  "tokenizer" should {

    "return correct answer" in {

      assert(
        transformed.get(NamedFeature[Array[Annotation]]("token")).get === Array(
          Annotation(TOKEN, 0, 1, "My", Map("sentence" -> "0")),
          Annotation(TOKEN, 3, 6, "name", Map("sentence" -> "0")),
          Annotation(TOKEN, 8, 9, "is", Map("sentence" -> "0")),
          Annotation(TOKEN, 11, 14, "Alex", Map("sentence" -> "0")),
          Annotation(TOKEN, 16, 19, "Wang", Map("sentence" -> "0")),
          Annotation(TOKEN, 20, 20, ".", Map("sentence" -> "0")),
          Annotation(TOKEN, 22, 25, "Here", Map("sentence" -> "1")),
          Annotation(TOKEN, 27, 28, "is", Map("sentence" -> "1")),
          Annotation(TOKEN, 30, 31, "my", Map("sentence" -> "1")),
          Annotation(TOKEN, 33, 37, "email", Map("sentence" -> "1")),
          Annotation(TOKEN, 38, 38, ":", Map("sentence" -> "1")),
          Annotation(TOKEN, 40, 58, "alex.wang@gmail.com", Map("sentence" -> "1")),
          Annotation(TOKEN, 59, 59, ".", Map("sentence" -> "1"))
        )
      )
    }
  }

  "NER" should {
    "return correct answer" in {
      val expected = Array(
        Annotation(NAMED_ENTITY, 0, 1, "O", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 3, 6, "O", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 8, 9, "O", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 11, 14, "B-PER", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 16, 19, "I-PER", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 20, 20, "O", Map("sentence" -> "0")),
        Annotation(NAMED_ENTITY, 22, 25, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 27, 28, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 30, 31, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 33, 37, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 38, 38, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 40, 58, "O", Map("sentence" -> "1")),
        Annotation(NAMED_ENTITY, 59, 59, "O", Map("sentence" -> "1"))
      )

      val preds = transformed.get(NamedFeature[Array[Annotation]]("ner")).get
      assert(preds.length == expected.length)
      preds.zip(expected).foreach {case (p, e) =>
        assert(p.annotatorType == e.annotatorType)
        assert(p.begin == e.begin)
        assert(p.end == e.end)
        assert(p.result == e.result)
      }
    }
  }

  "NER Converter" should {
    "return correct answer" in {
      val expected = Array(Annotation(CHUNK, 11, 19, "Alex Wang", Map.empty[String, String]))
      val preds = transformed.get(NamedFeature[Array[Annotation]]("predictions")).get
      assert(preds.length == expected.length)
      preds.zip(expected).foreach { case (p, e) =>
        assert(p.annotatorType == e.annotatorType)
        assert(p.begin == e.begin)
        assert(p.end == e.end)
        assert(p.result == e.result)
      }
    }
  }

}
