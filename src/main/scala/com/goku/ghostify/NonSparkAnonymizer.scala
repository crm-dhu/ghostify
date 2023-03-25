package com.goku.ghostify

import com.goku.ghostify.data.{FeatureMap, NamedFeature}
import com.goku.ghostify.nlp._
import com.goku.ghostify.util.Params
import com.johnsnowlabs.nlp.Annotation

object NonSparkAnonymizer {

  private final val InputCol = "text"
  private final val OutputCol = "predictions"

  def pipelineBuilder(): PortalPipeline = {
    val documentAssembler: DocumentAssembler =
      DocumentAssembler(
        NamedFeature[String](InputCol),
        NamedFeature[Array[Annotation]]("document")
      )
    val sentenceDetector: SentenceDetector = SentenceDetector(
      NamedFeature[Array[Annotation]]("document"),
      NamedFeature[Array[Annotation]]("sentence")
    )
    val tokenizer: TokenizerModel =
      TokenizerModel(
        NamedFeature[Array[Annotation]]("sentence"),
        NamedFeature[Array[Annotation]]("token")
      )

    val ner: BertForTokenClassification = BertForTokenClassification(
      (NamedFeature[Array[Annotation]]("sentence"), NamedFeature[Array[Annotation]]("token")),
      NamedFeature[Array[Annotation]]("ner"),
      Params.ModelPath
    )

    val nerConverter: NerConverter = NerConverter(
      (
        NamedFeature[Array[Annotation]]("document"),
        NamedFeature[Array[Annotation]]("token"),
        NamedFeature[Array[Annotation]]("ner")
      ),
      NamedFeature[Array[Annotation]](OutputCol)
    )
    PortalPipeline(Seq(documentAssembler, sentenceDetector, tokenizer, ner, nerConverter))
  }

  lazy val pipeline: PortalPipeline = pipelineBuilder()

  val featureMap: FeatureMap = FeatureMap()
  def apply(inputs: Seq[String]): Seq[String] = {
    inputs.map { r =>
      featureMap.put(NamedFeature(InputCol), r)
      val transformed = pipeline.transform(featureMap)
      val predictions = transformed.get(NamedFeature[Array[Annotation]](OutputCol)).get
      val pos = predictions.filter(_.metadata.contains("entity"))
        .map(p => (p.begin, p.end, p.metadata("entity")))
      val begin = 0 +: pos.map(_._2 + 1)
      val end = pos.map(_._1) :+ r.length
      val tag = pos.map(_._3) :+ ""
      val neTagged = begin
        .zip(end)
        .zip(tag)
        .map { case ((b, e), t) =>
          if (t.isEmpty) r.substring(b, e) else s"${r.substring(b, e)}[$t]"
        }
        .mkString("")
      Params.EmailRegex.replaceAllIn(neTagged, "[EMAIL]")
    }
  }
}
