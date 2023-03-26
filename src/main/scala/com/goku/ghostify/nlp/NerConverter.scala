package com.goku.ghostify.nlp

import com.goku.ghostify.PortalTernaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.johnsnowlabs.nlp.AnnotatorType.CHUNK
import com.johnsnowlabs.nlp.annotators.common.NerTagged
import com.johnsnowlabs.nlp.annotators.ner.NerTagsEncoding
import com.johnsnowlabs.nlp.{Annotation, AnnotatorType}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._

case class NerConverter(
  inputCols: (
    NamedFeature[Array[Annotation]],
    NamedFeature[Array[Annotation]],
    NamedFeature[Array[Annotation]]
  ),
  outputCol: NamedFeature[Array[Annotation]]
) extends PortalTernaryTransformer[Array[Annotation], Array[Annotation], Array[Annotation], Array[
      Annotation
    ]] {
  override def transformFeature(
    annotations: (Array[Annotation], Array[Annotation], Array[Annotation])
  ): Array[Annotation] = {
    val (docs, tokens, nes) = annotations
    val sentences = NerTagged.unpack(docs ++ tokens ++ nes)
    val cleanDocs = docs.filter(a =>
      a.annotatorType == AnnotatorType.DOCUMENT && sentences.exists(b =>
        b.indexedTaggedWords.exists(c => c.begin >= a.begin && c.end <= a.end)
      )
    )

    val entities = sentences.zip(cleanDocs.zipWithIndex).flatMap { case (sentence, doc) =>
      NerTagsEncoding.fromIOB(sentence, doc._1, sentenceIndex = doc._2, true)
    }

    entities.zipWithIndex.map { case (entity, idx) =>
      val baseMetadata =
        Map("entity" -> entity.entity, "sentence" -> entity.sentenceId, "chunk" -> idx.toString)
      val metadata =
        if (entity.confidence.isEmpty) baseMetadata
        else baseMetadata + ("confidence" -> entity.confidence.get.toString)
      Annotation(CHUNK, entity.start, entity.end, entity.text, metadata)
    }.toArray
  }

  override def marshal: Json = this.asJson
}
