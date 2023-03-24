package com.goku.ghostify.nlp.annotators.common

import scala.collection.Map

import com.goku.ghostify.nlp.{Annotation, AnnotatorType}

case class Sentence(
  content: String,
  start: Int,
  end: Int,
  index: Int,
  metadata: Option[Map[String, String]] = None
)

object Sentence {
  def fromTexts(texts: String*): Seq[Sentence] = {
    var idx = 0
    texts.zipWithIndex.map { case (text, textIndex) =>
      val sentence = Sentence(text, idx, idx + text.length - 1, textIndex)
      idx += text.length + 1
      sentence
    }
  }
}

object SentenceSplit {
  def annotatorType: String = AnnotatorType.DOCUMENT

  def unpack(annotations: Seq[Annotation]): Seq[Sentence] = {
    annotations.filter(_.annotatorType == annotatorType).map { annotation =>
      val index: Int = annotation.metadata.getOrElse("sentence", "0").toInt
      Sentence(
        annotation.result,
        annotation.begin,
        annotation.end,
        index,
        Option(annotation.metadata)
      )
    }
  }

  def pack(items: Seq[Sentence]): Seq[Annotation] = {
    items.sortBy(i => i.start).zipWithIndex.map { case (item, index) =>
      Annotation(
        annotatorType,
        item.start,
        item.end,
        item.content,
        Map("sentence" -> index.toString)
      )
    }
  }
}
