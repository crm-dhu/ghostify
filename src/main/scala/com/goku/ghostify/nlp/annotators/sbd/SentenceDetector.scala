package com.goku.ghostify.nlp.annotators.sbd

import com.goku.ghostify.nlp.{Annotation, AnnotatorModel, HasSimpleAnnotate}
import com.goku.ghostify.nlp.annotators.common.{Sentence, SentenceSplit}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}

import com.salesforce.mce.ghostify.nlp._

class SentenceDetector(override val uid: String)
    extends AnnotatorModel[SentenceDetector] with HasSimpleAnnotate[SentenceDetector]
    with SentenceDetectorParams {

  import com.johnsnowlabs.nlp.AnnotatorType._

  def this() = this(Identifiable.randomUID("SENTENCE"))

  override val outputAnnotatorType: AnnotatorType = DOCUMENT

  override val inputAnnotatorTypes: Array[AnnotatorType] = Array(DOCUMENT)

  def tag(document: String): Array[Sentence] = {
    new SimpleSentenceExtractor(document).pull
      .flatMap(sentence => {
        var currentStart = sentence.start
        get(splitLength)
          .map(splitLength => truncateSentence(sentence.content, splitLength))
          .getOrElse(Array(sentence.content))
          .zipWithIndex
          .map { case (truncatedSentence, index) =>
            val currentEnd = currentStart + truncatedSentence.length - 1
            val result = Sentence(truncatedSentence, currentStart, currentEnd, index)

            currentStart = currentEnd + 2
            result
          }
      })
  }

  override def beforeAnnotate(dataset: Dataset[_]): Dataset[_] = {

    dataset
  }

  override def annotate(annotations: Seq[Annotation]): Seq[Annotation] = {
    val docs = annotations.map(_.result)
    val sentences = docs
      .flatMap(doc => tag(doc))
      .filter(t =>
        t.content.nonEmpty && t.content.length >= $(minLength) && get(maxLength).forall(m =>
          t.content.length <= m
        )
      )
    SentenceSplit.pack(sentences)
  }

  override protected def afterAnnotate(dataset: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{array, col, explode}
    if ($(explodeSentences)) {
      dataset
        .select(
          dataset.columns.filterNot(_ == getOutputCol).map(col) :+ explode(col(getOutputCol))
            .as("_tmp"): _*
        )
        .withColumn(
          getOutputCol,
          array(col("_tmp"))
            .as(getOutputCol, dataset.schema.fields.find(_.name == getOutputCol).get.metadata)
        )
        .drop("_tmp")
    } else dataset
  }

}

object SentenceDetector extends DefaultParamsReadable[SentenceDetector]
