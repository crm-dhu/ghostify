package com.goku.ghostify.nlp

import com.goku.ghostify.PortalUnaryTransformer
import com.goku.ghostify.data.NamedFeature
import com.goku.ghostify.util.ObjectMarshaller
import com.johnsnowlabs.nlp.Annotation
import com.johnsnowlabs.nlp.annotators.common.{Sentence, SentenceSplit}
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.DefaultPragmaticMethod
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
case class SentenceDetector(
  inputCol: NamedFeature[Array[Annotation]],
  outputCol: NamedFeature[Array[Annotation]]
) extends PortalUnaryTransformer[Array[Annotation], Array[Annotation]] {

  override def transformFeature(annotations: Array[Annotation]): Array[Annotation] = {
    val docs = annotations.map(_.result)
    val sentences = docs
      .flatMap(doc => tag(doc))
      .filter(t => t.content.nonEmpty)
    SentenceSplit.pack(sentences).toArray
  }

  lazy val model = new DefaultPragmaticMethod(true, true)
  def tag(document: String): Array[Sentence] = {
    model
      .extractBounds(document)
      .flatMap(sentence => {
        var currentStart = sentence.start
        Array(sentence.content).zipWithIndex
          .map { case (truncatedSentence, index) =>
            val currentEnd = currentStart + truncatedSentence.length - 1
            val result = Sentence(truncatedSentence, currentStart, currentEnd, index)

            /**
             * +1 because of shifting to the next token begin. +1 because of a whitespace jump to
             * next token.
             */
            currentStart = currentEnd + 2
            result
          }
      })
  }

  def marshal = this.asJson
}

object SentenceDetector extends ObjectMarshaller[SentenceDetector] {

  def unmarshal(jsonObj: Json): Either[Throwable, SentenceDetector] = jsonObj.as[SentenceDetector]
}
