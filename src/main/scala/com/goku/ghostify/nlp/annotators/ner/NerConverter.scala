package com.goku.ghostify.nlp.annotators.ner

import com.goku.ghostify.nlp.annotators.common.NerTagged
import com.goku.ghostify.nlp.{Annotation, AnnotatorModel, AnnotatorType, HasSimpleAnnotate}
import org.apache.spark.ml.param.{BooleanParam, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import AnnotatorType.{CHUNK, DOCUMENT, NAMED_ENTITY, TOKEN}

import com.salesforce.mce.ghostify.nlp._
class NerConverter(override val uid: String)
    extends AnnotatorModel[NerConverter] with HasSimpleAnnotate[NerConverter] {

  def this() = this(Identifiable.randomUID("NER_CONVERTER"))

  override val inputAnnotatorTypes: Array[String] = Array(DOCUMENT, TOKEN, NAMED_ENTITY)

  override val outputAnnotatorType: AnnotatorType = CHUNK

  val whiteList: StringArrayParam = new StringArrayParam(
    this,
    "whiteList",
    "If defined, list of entities to process. The rest will be ignored. Do not include IOB prefix on labels"
  )

  def setWhiteList(list: String*): NerConverter.this.type = set(whiteList, list.toArray)

  val preservePosition: BooleanParam = new BooleanParam(
    this,
    "preservePosition",
    "Whether to preserve the original position of the tokens in the original document or use the modified tokens"
  )

  def setPreservePosition(value: Boolean): this.type = set(preservePosition, value)

  setDefault(preservePosition -> true)

  override def annotate(annotations: Seq[Annotation]): Seq[Annotation] = {
    val sentences = NerTagged.unpack(annotations)
    val docs = annotations.filter(a =>
      a.annotatorType == AnnotatorType.DOCUMENT && sentences.exists(b =>
        b.indexedTaggedWords.exists(c => c.begin >= a.begin && c.end <= a.end)
      )
    )

    val entities = sentences.zip(docs.zipWithIndex).flatMap { case (sentence, doc) =>
      NerTagsEncoding.fromIOB(sentence, doc._1, sentenceIndex = doc._2, $(preservePosition))
    }

    entities
      .filter(entity => get(whiteList).forall(validEntity => validEntity.contains(entity.entity)))
      .zipWithIndex
      .map { case (entity, idx) =>
        val baseMetadata =
          Map("entity" -> entity.entity, "sentence" -> entity.sentenceId, "chunk" -> idx.toString)
        val metadata =
          if (entity.confidence.isEmpty) baseMetadata
          else baseMetadata + ("confidence" -> entity.confidence.get.toString)
        Annotation(outputAnnotatorType, entity.start, entity.end, entity.text, metadata)

      }
  }

}

object NerConverter extends DefaultParamsReadable[NerConverter]
